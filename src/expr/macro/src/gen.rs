// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generate code for the functions.

use itertools::Itertools;
use proc_macro2::Span;
use quote::{format_ident, quote};

use super::*;

impl FunctionAttr {
    /// Expands the wildcard in function arguments or return type.
    pub fn expand(&self) -> Vec<Self> {
        let args = self.args.iter().map(|ty| types::expand_type_wildcard(ty));
        let ret = types::expand_type_wildcard(&self.ret);
        // multi_cartesian_product should emit an empty set if the input is empty.
        let args_cartesian_product =
            args.multi_cartesian_product()
                .chain(match self.args.is_empty() {
                    true => vec![vec![]],
                    false => vec![],
                });
        let mut attrs = Vec::new();
        for (args, mut ret) in args_cartesian_product.cartesian_product(ret) {
            if ret == "auto" {
                ret = types::min_compatible_type(&args);
            }
            let attr = FunctionAttr {
                args: args.iter().map(|s| s.to_string()).collect(),
                ret: ret.to_string(),
                ..self.clone()
            };
            attrs.push(attr);
        }
        attrs
    }

    /// Generate a descriptor of the function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_descriptor(
        &self,
        user_fn: &UserFunctionAttr,
        build_fn: bool,
    ) -> Result<TokenStream2> {
        if self.is_table_function {
            return self.generate_table_function_descriptor(user_fn, build_fn);
        }
        let name = self.name.clone();
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { crate::sig::func::FuncSign };
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_fn(user_fn)?
        };
        let deprecated = self.deprecated;
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { crate::sig::func::_register(#descriptor_type {
                    func: risingwave_pb::expr::expr_node::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                    deprecated: #deprecated,
                }) };
            }
        })
    }

    fn generate_build_fn(&self, user_fn: &UserFunctionAttr) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let fn_name = format_ident!("{}", user_fn.name);
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", types::array_type(t)));
        let ret_array = format_ident!("{}", types::array_type(&self.ret));
        let arg_types = self
            .args
            .iter()
            .map(|t| types::ref_type(t).parse::<TokenStream2>().unwrap());
        let ret_type = types::ref_type(&self.ret).parse::<TokenStream2>().unwrap();
        let exprs = (0..num_args)
            .map(|i| format_ident!("e{i}"))
            .collect::<Vec<_>>();
        #[expect(
            clippy::redundant_clone,
            reason = "false positive https://github.com/rust-lang/rust-clippy/issues/10545"
        )]
        let exprs0 = exprs.clone();

        let build_expr = if self.ret == "varchar" && user_fn.is_writer_style() {
            let template_struct = match num_args {
                1 => format_ident!("UnaryBytesExpression"),
                2 => format_ident!("BinaryBytesExpression"),
                3 => format_ident!("TernaryBytesExpression"),
                4 => format_ident!("QuaternaryBytesExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let args = (0..=num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let func = match user_fn.return_type {
                ReturnType::T => quote! { Ok(#fn_name(#(#args1),*)) },
                ReturnType::Result => quote! { #fn_name(#(#args1),*) },
                _ => todo!("returning Option is not supported yet"),
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays),*, _>::new(
                    #(#exprs),*,
                    return_type,
                    |#(#args),*| #func,
                )))
            }
        } else if self.args.iter().all(|t| t == "boolean")
            && self.ret == "boolean"
            && !user_fn.return_type.contains_result()
            && self.batch_fn.is_some()
        {
            let template_struct = match num_args {
                1 => format_ident!("BooleanUnaryExpression"),
                2 => format_ident!("BooleanBinaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let batch_fn = format_ident!("{}", self.batch_fn.as_ref().unwrap());
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let func = if user_fn.arg_option && user_fn.return_type == ReturnType::Option {
                quote! { #fn_name(#(#args1),*) }
            } else if user_fn.arg_option {
                quote! { Some(#fn_name(#(#args1),*)) }
            } else {
                let args2 = args.clone();
                let args3 = args.clone();
                quote! {
                    match (#(#args1),*) {
                        (#(Some(#args2)),*) => Some(#fn_name(#(#args3),*)),
                        _ => None,
                    }
                }
            };
            quote! {
                Ok(Box::new(crate::expr::template_fast::#template_struct::new(
                    #(#exprs,)*
                    #batch_fn,
                    |#(#args),*| #func,
                )))
            }
        } else if self.args.len() == 2 && self.ret == "boolean" && user_fn.is_pure() {
            let compatible_type = types::ref_type(types::min_compatible_type(&self.args))
                .parse::<TokenStream2>()
                .unwrap();
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let generic = if user_fn.generic == 3 {
                // XXX: for generic compare functions, we need to specify the compatible type
                quote! { ::<_, _, #compatible_type> }
            } else {
                quote! {}
            };
            quote! {
                Ok(Box::new(crate::expr::template_fast::CompareExpression::<_, #(#arg_arrays),*>::new(
                    #(#exprs,)*
                    |#(#args),*| #fn_name #generic(#(#args1),*),
                )))
            }
        } else if self.args.iter().all(|t| types::is_primitive(t)) && user_fn.is_pure() {
            let template_struct = match num_args {
                0 => format_ident!("NullaryExpression"),
                1 => format_ident!("UnaryExpression"),
                2 => format_ident!("BinaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            quote! {
                Ok(Box::new(crate::expr::template_fast::#template_struct::<_, #(#arg_types,)* #ret_type>::new(
                    #(#exprs,)*
                    return_type,
                    #fn_name,
                )))
            }
        } else if user_fn.arg_option || user_fn.return_type.contains_option() {
            let template_struct = match num_args {
                1 => format_ident!("UnaryNullableExpression"),
                2 => format_ident!("BinaryNullableExpression"),
                3 => format_ident!("TernaryNullableExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let generic = if user_fn.generic == 3 {
                // XXX: for generic compare functions, we need to specify the compatible type
                let compatible_type = types::ref_type(types::min_compatible_type(&self.args))
                    .parse::<TokenStream2>()
                    .unwrap();
                quote! { ::<_, _, #compatible_type> }
            } else {
                quote! {}
            };
            let mut func = quote! { #fn_name #generic(#(#args1),*) };
            func = match user_fn.return_type {
                ReturnType::T => quote! { Ok(Some(#func)) },
                ReturnType::Option => quote! { Ok(#func) },
                ReturnType::Result => quote! { #func.map(Some) },
                ReturnType::ResultOption => quote! { #func },
            };
            if !user_fn.arg_option {
                let args2 = args.clone();
                let args3 = args.clone();
                func = quote! {
                    match (#(#args2),*) {
                        (#(Some(#args3)),*) => #func,
                        _ => Ok(None),
                    }
                };
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays,)* #ret_array, _>::new(
                    #(#exprs,)*
                    return_type,
                    |#(#args),*| #func,
                )))
            }
        } else {
            let template_struct = match num_args {
                0 => format_ident!("NullaryExpression"),
                1 => format_ident!("UnaryExpression"),
                2 => format_ident!("BinaryExpression"),
                3 => format_ident!("TernaryExpression"),
                _ => return Err(Error::new(Span::call_site(), "unsupported arguments")),
            };
            let args = (0..num_args).map(|i| format_ident!("x{i}"));
            let args1 = args.clone();
            let func = match user_fn.return_type {
                ReturnType::T => quote! { Ok(#fn_name(#(#args1),*)) },
                ReturnType::Result => quote! { #fn_name(#(#args1),*) },
                _ => panic!("return type should not contain Option"),
            };
            quote! {
                Ok(Box::new(crate::expr::template::#template_struct::<#(#arg_arrays,)* #ret_array, _>::new(
                    #(#exprs,)*
                    return_type,
                    |#(#args),*| #func,
                )))
            }
        };
        Ok(quote! {
            |return_type, children| {
                use risingwave_common::array::*;
                use risingwave_common::types::*;

                crate::ensure!(children.len() == #num_args);
                let mut iter = children.into_iter();
                #(let #exprs0 = iter.next().unwrap();)*

                #build_expr
            }
        })
    }

    /// Generate a descriptor of the aggregate function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_agg_descriptor(
        &self,
        user_fn: &UserFunctionAttr,
        build_fn: bool,
    ) -> Result<TokenStream2> {
        let name = self.name.clone();

        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { crate::sig::agg::AggFuncSig };
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else {
            self.generate_agg_build_fn(user_fn)?
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { crate::sig::agg::_register(#descriptor_type {
                    func: crate::agg::AggKind::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                }) };
            }
        })
    }

    /// Generate build function for aggregate function.
    fn generate_agg_build_fn(&self, user_fn: &UserFunctionAttr) -> Result<TokenStream2> {
        let state_type: TokenStream2 = match &self.state {
            Some(state) if state == "ref" => types::ref_type(&self.ret).parse().unwrap(),
            Some(state) if state != "ref" => state.parse().unwrap(),
            _ => types::owned_type(&self.ret).parse().unwrap(),
        };
        let let_arrays = self
            .args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                let array = format_ident!("a{i}");
                let variant: TokenStream2 = types::variant(arg).parse().unwrap();
                quote! {
                    let ArrayImpl::#variant(#array) = &**input.column_at(#i) else {
                        bail!("input type mismatch. expect: {}", stringify!(#variant));
                    };
                }
            })
            .collect_vec();
        let let_values = (0..self.args.len())
            .map(|i| {
                let v = format_ident!("v{i}");
                let a = format_ident!("a{i}");
                quote! { let #v = unsafe { #a.value_at_unchecked(row_id) }; }
            })
            .collect_vec();
        let let_state = match &self.state {
            Some(s) if s == "ref" => {
                quote! { state0.as_ref().map(|x| x.as_scalar_ref_impl().try_into().unwrap()) }
            }
            _ => quote! { state0.take().map(|s| s.try_into().unwrap()) },
        };
        let assign_state = match &self.state {
            Some(s) if s == "ref" => quote! { state.map(|x| x.to_owned_scalar().into()) },
            _ => quote! { state.map(|s| s.into()) },
        };
        let create_state = self.init_state.as_ref().map(|state| {
            let state: TokenStream2 = state.parse().unwrap();
            quote! {
                fn create_state(&self) -> AggregateState {
                    AggregateState::Datum(Some(#state.into()))
                }
            }
        });
        let fn_name = format_ident!("{}", user_fn.name);
        let args = (0..self.args.len()).map(|i| format_ident!("v{i}"));
        let args = quote! { #(#args,)* };
        let retract = match user_fn.retract {
            true => quote! { matches!(op, Op::Delete | Op::UpdateDelete) },
            false => quote! {},
        };
        let check_retract = match user_fn.retract {
            true => quote! {},
            false => {
                let msg = format!("aggregate function {} only supports append", self.name);
                quote! { assert_eq!(op, Op::Insert, #msg); }
            }
        };
        let mut next_state = quote! { #fn_name(state, #args #retract) };
        next_state = match user_fn.return_type {
            ReturnType::T => quote! { Some(#next_state) },
            ReturnType::Option => next_state,
            ReturnType::Result => quote! { Some(#next_state?) },
            ReturnType::ResultOption => quote! { #next_state? },
        };
        if !user_fn.arg_option {
            match self.args.len() {
                0 => {
                    next_state = quote! {
                        match state {
                            Some(state) => #next_state,
                            None => state,
                        }
                    };
                }
                1 => {
                    let first_state = match &self.init_state {
                        Some(_) => quote! { unreachable!() },
                        _ => quote! { Some(v0.into()) },
                    };
                    next_state = quote! {
                        match (state, v0) {
                            (Some(state), Some(v0)) => #next_state,
                            (None, Some(v0)) => #first_state,
                            (state, None) => state,
                        }
                    };
                }
                _ => todo!("multiple arguments are not supported for non-option function"),
            }
        }

        Ok(quote! {
            |agg| {
                use std::collections::HashSet;
                use std::ops::Range;
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::bail;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::estimate_size::EstimateSize;

                use crate::Result;
                use crate::agg::AggregateState;

                #[derive(Clone)]
                struct Agg {
                    return_type: DataType,
                }

                #[async_trait::async_trait]
                impl crate::agg::AggregateFunction for Agg {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }

                    #create_state

                    async fn update(&self, state0: &mut AggregateState, input: &StreamChunk) -> Result<()> {
                        #(#let_arrays)*
                        let state0 = state0.as_datum_mut();
                        let mut state: Option<#state_type> = #let_state;
                        match input.vis() {
                            Vis::Bitmap(bitmap) => {
                                for row_id in bitmap.iter_ones() {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                            Vis::Compact(_) => {
                                for row_id in 0..input.capacity() {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                        }
                        *state0 = #assign_state;
                        Ok(())
                    }

                    async fn update_range(&self, state0: &mut AggregateState, input: &StreamChunk, range: Range<usize>) -> Result<()> {
                        assert!(range.end <= input.capacity());
                        #(#let_arrays)*
                        let state0 = state0.as_datum_mut();
                        let mut state: Option<#state_type> = #let_state;
                        match input.vis() {
                            Vis::Bitmap(bitmap) => {
                                for row_id in bitmap.iter_ones() {
                                    if row_id < range.start {
                                        continue;
                                    } else if row_id >= range.end {
                                        break;
                                    }
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                            Vis::Compact(_) => {
                                for row_id in range {
                                    let op = unsafe { *input.ops().get_unchecked(row_id) };
                                    #check_retract
                                    #(#let_values)*
                                    state = #next_state;
                                }
                            }
                        }
                        *state0 = #assign_state;
                        Ok(())
                    }

                    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
                        Ok(state.as_datum().clone())
                    }
                }

                Ok(Box::new(Agg {
                    return_type: agg.return_type.clone(),
                }))
            }
        })
    }

    /// Generate a descriptor of the table function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    fn generate_table_function_descriptor(
        &self,
        user_fn: &UserFunctionAttr,
        build_fn: bool,
    ) -> Result<TokenStream2> {
        let name = self.name.clone();
        let mut args = Vec::with_capacity(self.args.len());
        for ty in &self.args {
            args.push(data_type_name(ty));
        }
        let ret = data_type_name(&self.ret);

        let pb_type = format_ident!("{}", utils::to_camel_case(&name));
        let ctor_name = format_ident!("{}", self.ident_name());
        let descriptor_type = quote! { crate::sig::table_function::FuncSign };
        let build_fn = if build_fn {
            let name = format_ident!("{}", user_fn.name);
            quote! { #name }
        } else {
            self.generate_build_table_function(user_fn)?
        };
        let type_infer_fn = if let Some(func) = &self.type_infer {
            func.parse().unwrap()
        } else {
            if matches!(self.ret.as_str(), "any" | "list" | "struct") {
                return Err(Error::new(
                    Span::call_site(),
                    format!("type inference function is required for {}", self.ret),
                ));
            }
            let ty = data_type(&self.ret);
            quote! { |_| Ok(#ty) }
        };
        Ok(quote! {
            #[ctor::ctor]
            fn #ctor_name() {
                use risingwave_common::types::{DataType, DataTypeName};
                unsafe { crate::sig::table_function::_register(#descriptor_type {
                    func: risingwave_pb::expr::table_function::Type::#pb_type,
                    inputs_type: &[#(#args),*],
                    ret_type: #ret,
                    build: #build_fn,
                    type_infer: #type_infer_fn,
                }) };
            }
        })
    }

    fn generate_build_table_function(&self, user_fn: &UserFunctionAttr) -> Result<TokenStream2> {
        let num_args = self.args.len();
        let return_types = output_types(&self.ret);
        let fn_name = format_ident!("{}", user_fn.name);
        let struct_name = format_ident!("{}", self.ident_name());
        let arg_ids = (0..num_args)
            .filter(|i| match &self.prebuild {
                Some(s) => !s.contains(&format!("${i}")),
                None => true,
            })
            .collect_vec();
        let const_ids = (0..num_args).filter(|i| match &self.prebuild {
            Some(s) => s.contains(&format!("${i}")),
            None => false,
        });
        let inputs: Vec<_> = arg_ids.iter().map(|i| format_ident!("i{i}")).collect();
        let all_child: Vec<_> = (0..num_args).map(|i| format_ident!("child{i}")).collect();
        let const_child: Vec<_> = const_ids.map(|i| format_ident!("child{i}")).collect();
        let child: Vec<_> = arg_ids.iter().map(|i| format_ident!("child{i}")).collect();
        let array_refs: Vec<_> = arg_ids.iter().map(|i| format_ident!("array{i}")).collect();
        let arrays: Vec<_> = arg_ids.iter().map(|i| format_ident!("a{i}")).collect();
        let arg_arrays = self
            .args
            .iter()
            .map(|t| format_ident!("{}", types::array_type(t)));
        let outputs = (0..return_types.len())
            .map(|i| format_ident!("o{i}"))
            .collect_vec();
        let builders = (0..return_types.len())
            .map(|i| format_ident!("builder{i}"))
            .collect_vec();
        let builder_types = return_types
            .iter()
            .map(|ty| format_ident!("{}Builder", types::array_type(ty)))
            .collect_vec();
        let return_types = if return_types.len() == 1 {
            vec![quote! { self.return_type.clone() }]
        } else {
            (0..return_types.len())
                .map(|i| quote! { self.return_type.as_struct().types().nth(#i).unwrap().clone() })
                .collect()
        };
        let build_value_array = if return_types.len() == 1 {
            quote! { let [value_array] = value_arrays; }
        } else {
            quote! {
                let bitmap = value_arrays[0].null_bitmap().clone();
                let value_array = StructArray::new(
                    self.return_type.as_struct().clone(),
                    value_arrays.to_vec(),
                    bitmap,
                ).into_ref();
            }
        };
        let const_arg = match &self.prebuild {
            Some(_) => quote! { &self.const_arg },
            None => quote! {},
        };
        let const_arg_type = match &self.prebuild {
            Some(s) => s.split("::").next().unwrap().parse().unwrap(),
            None => quote! { () },
        };
        let const_arg_value = match &self.prebuild {
            Some(s) => s
                .replace('$', "child")
                .parse()
                .expect("invalid prebuild syntax"),
            None => quote! { () },
        };
        let iter = match user_fn.return_type {
            ReturnType::T => quote! { iter },
            ReturnType::Option => quote! { iter.flatten() },
            ReturnType::Result => quote! { iter? },
            ReturnType::ResultOption => quote! { value?.flatten() },
        };
        let iterator_item_type = user_fn.iterator_item_type.clone().ok_or_else(|| {
            Error::new(
                user_fn.return_type_span,
                "expect `impl Iterator` in return type",
            )
        })?;
        let output = match iterator_item_type {
            ReturnType::T => quote! { Some(output) },
            ReturnType::Option => quote! { output },
            ReturnType::Result => quote! { Some(output?) },
            ReturnType::ResultOption => quote! { output? },
        };

        Ok(quote! {
            |return_type, chunk_size, children| {
                use risingwave_common::array::*;
                use risingwave_common::types::*;
                use risingwave_common::buffer::Bitmap;
                use risingwave_common::util::iter_util::ZipEqFast;
                use itertools::multizip;

                crate::ensure!(children.len() == #num_args);
                let mut iter = children.into_iter();
                #(let #all_child = iter.next().unwrap();)*
                #(let #const_child = #const_child.eval_const()?;)*

                #[derive(Debug)]
                #[allow(non_camel_case_types)]
                struct #struct_name {
                    return_type: DataType,
                    chunk_size: usize,
                    #(#child: BoxedExpression,)*
                    const_arg: #const_arg_type,
                }
                #[async_trait::async_trait]
                impl crate::table_function::TableFunction for #struct_name {
                    fn return_type(&self) -> DataType {
                        self.return_type.clone()
                    }
                    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
                        self.eval_inner(input)
                    }
                }
                impl #struct_name {
                    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
                    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
                        #(
                        let #array_refs = self.#child.eval_checked(input).await?;
                        let #arrays: &#arg_arrays = #array_refs.as_ref().into();
                        )*

                        let mut index_builder = I32ArrayBuilder::new(self.chunk_size);
                        #(let mut #builders = #builder_types::with_type(self.chunk_size, #return_types);)*

                        for (i, (row, visible)) in multizip((#(#arrays.iter(),)*)).zip_eq_fast(input.vis().iter()).enumerate() {
                            if let (#(Some(#inputs),)*) = row && visible {
                                let iter = #fn_name(#(#inputs,)* #const_arg);
                                for output in #iter {
                                    index_builder.append(Some(i as i32));
                                    match #output {
                                        Some((#(#outputs),*)) => { #(#builders.append(Some(#outputs.as_scalar_ref()));)* }
                                        None => { #(#builders.append_null();)* }
                                    }

                                    if index_builder.len() == self.chunk_size {
                                        let index_array = std::mem::replace(&mut index_builder, I32ArrayBuilder::new(self.chunk_size)).finish().into_ref();
                                        let value_arrays = [#(std::mem::replace(&mut #builders, #builder_types::with_type(self.chunk_size, #return_types)).finish().into_ref()),*];
                                        #build_value_array
                                        yield DataChunk::new(vec![index_array, value_array], self.chunk_size);
                                    }
                                }
                            }
                        }

                        if index_builder.len() > 0 {
                            let len = index_builder.len();
                            let index_array = index_builder.finish().into_ref();
                            let value_arrays = [#(#builders.finish().into_ref()),*];
                            #build_value_array
                            yield DataChunk::new(vec![index_array, value_array], len);
                        }
                    }
                }

                Ok(Box::new(#struct_name {
                    return_type,
                    chunk_size,
                    #(#child,)*
                    const_arg: #const_arg_value,
                }))
            }
        })
    }
}

fn data_type_name(ty: &str) -> TokenStream2 {
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { DataTypeName::#variant }
}

fn data_type(ty: &str) -> TokenStream2 {
    if let Some(ty) = ty.strip_suffix("[]") {
        let inner_type = data_type(ty);
        return quote! { DataType::List(Box::new(#inner_type)) };
    }
    if ty.starts_with("struct<") {
        return quote! { DataType::Struct(#ty.parse().expect("invalid struct type")) };
    }
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { DataType::#variant }
}

/// Extract multiple output types.
///
/// ```ignore
/// output_types("int32") -> ["int32"]
/// output_types("struct<key varchar, value jsonb>") -> ["varchar", "jsonb"]
/// ```
fn output_types(ty: &str) -> Vec<&str> {
    if let Some(s) = ty.strip_prefix("struct<") && let Some(args) = s.strip_suffix('>') {
        args.split(',').map(|s| s.split_whitespace().nth(1).unwrap()).collect()
    } else {
        vec![ty]
    }
}
