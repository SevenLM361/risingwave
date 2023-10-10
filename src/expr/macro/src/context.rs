use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Error, FnArg, Ident, ItemFn, Result, Token, Type, Visibility};

/// See [`super::define_context`].
#[derive(Debug, Clone)]
pub(super) struct DefineContextField {
    vis: Visibility,
    name: Ident,
    ty: Type,
}

/// See [`super::define_context`].
#[derive(Debug, Clone)]
pub(super) struct DefineContextAttr {
    fields: Vec<DefineContextField>,
}

impl Parse for DefineContextField {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let vis: Visibility = input.parse()?;
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;

        Ok(Self { vis, name, ty })
    }
}

impl Parse for DefineContextAttr {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let fields = input.parse_terminated(DefineContextField::parse, Token![,])?;
        Ok(Self {
            fields: fields.into_iter().collect(),
        })
    }
}

impl DefineContextField {
    pub(super) fn gen(self) -> Result<TokenStream> {
        let Self { vis, name, ty } = self;

        {
            let name_s = name.to_string();
            if name_s.to_uppercase() != name_s {
                return Err(Error::new_spanned(
                    name,
                    "the name of context variable should be uppercase",
                ));
            }
        }

        Ok(quote! {
            #[allow(non_snake_case)]
            pub mod #name {
                pub type Type = #ty;

                tokio::task_local! {
                    static LOCAL_KEY: #ty;
                }

                #vis fn try_with<F, R>(f: F) -> Result<R, risingwave_expr::ContextUnavailable>
                where
                    F: FnOnce(&#ty) -> R
                {
                    LOCAL_KEY.try_with(f).map_err(|_| risingwave_expr::ContextUnavailable::new(stringify!(#name)))
                }

                pub fn scope<F>(value: #ty, f: F) -> tokio::task::futures::TaskLocalFuture<#ty, F>
                where
                    F: std::future::Future
                {
                    LOCAL_KEY.scope(value, f)
                }
            }
        })
    }
}

impl DefineContextAttr {
    pub(super) fn gen(self) -> Result<TokenStream> {
        let generated_fields: Vec<TokenStream> = self
            .fields
            .into_iter()
            .map(DefineContextField::gen)
            .try_collect()?;
        Ok(quote! {
            #(#generated_fields)*
        })
    }
}

pub struct CaptureContextAttr {
    /// The context variables which are captured.
    captures: Vec<Ident>,
}

impl Parse for CaptureContextAttr {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let captures = input.parse_terminated(Ident::parse, Token![,])?;
        Ok(Self {
            captures: captures.into_iter().collect(),
        })
    }
}

pub(super) fn generate_captured_function(
    attr: CaptureContextAttr,
    mut user_fn: ItemFn,
) -> Result<TokenStream> {
    let CaptureContextAttr { captures } = attr;
    let orig_user_fn = user_fn.clone();

    let sig = &mut user_fn.sig;

    // Modify the name.
    {
        let new_name = format!("{}_captured", sig.ident);
        let new_name = Ident::new(&new_name, sig.ident.span());
        sig.ident = new_name;
    }

    // Modify the inputs of sig.
    let inputs = &mut sig.inputs;
    if inputs.len() < captures.len() {
        return Err(syn::Error::new_spanned(
            inputs,
            format!("expected at least {} inputs", captures.len()),
        ));
    }

    let (captured_inputs, remained_inputs) = {
        let inputs = inputs.iter().cloned().by_ref();
        let captured_inputs = inputs.take(captures.len()).collect_vec();
        let remained_inputs = inputs.collect_vec();
        (captured_inputs, remained_inputs)
    };
    *inputs = remained_inputs.into_iter().collect();

    // Modify the body
    let body = &mut user_fn.block;
    let new_body = {
        let mut scoped = quote! { #body };

        #[allow(clippy::disallowed_methods)]
        for (context, arg) in captures.into_iter().zip(captured_inputs.into_iter()) {
            let FnArg::Typed(arg) = arg else {
                return syn::Error::new_spanned(
                    arg,
                    "receiver is not allowed in captured function",
                );
            };
            let name = format!("{pat}");
            scoped = quote_spanned! { context=>
                // TODO: Can we add an assertion here that `#context::Type`` is same as `#arg.ty`?
                #context::try_with(|#name| {
                    #scoped
                }).flatten()
            }
        }
        scoped
    };
    *body = new_body;

    Ok(quote! {
        #orig_user_fn
        #user_fn
    })
}
