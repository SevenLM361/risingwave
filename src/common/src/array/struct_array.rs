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

use core::fmt;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use either::Either;
use itertools::Itertools;
use risingwave_pb::data::{PbArray, PbArrayType, StructArrayData};

use super::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayResult};
use crate::array::ArrayRef;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;
use crate::types::{hash_datum, DataType, Datum, DatumRef, Scalar, StructType, ToDatumRef, ToText};
use crate::util::iter_util::ZipEqFast;
use crate::util::memcmp_encoding;
use crate::util::value_encoding::estimate_serialize_datum_size;

macro_rules! iter_fields_ref {
    ($self:expr, $it:ident, { $($body:tt)* }) => {
        iter_fields_ref!($self, $it, { $($body)* }, { $($body)* })
    };

    ($self:expr, $it:ident, { $($l_body:tt)* }, { $($r_body:tt)* }) => {
        match $self {
            StructRef::Indexed { arr, idx } => {
                let $it = arr.children.iter().map(move |a| a.value_at(idx));
                $($l_body)*
            }
            StructRef::ValueRef { val } => {
                let $it = val.fields.iter().map(ToDatumRef::to_datum_ref);
                $($r_body)*
            }
        }
    }
}

#[derive(Debug)]
pub struct StructArrayBuilder {
    bitmap: BitmapBuilder,
    pub(super) children_array: Vec<ArrayBuilderImpl>,
    type_: Arc<StructType>,
    len: usize,
}

impl ArrayBuilder for StructArrayBuilder {
    type ArrayType = StructArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Self {
        panic!("Must use with_type.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_type(
            capacity,
            DataType::Struct(Arc::new(StructType::new(vec![]))),
        )
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        let DataType::Struct(ty) = ty else {
            panic!("must be DataType::Struct");
        };
        let children_array = ty
            .fields
            .iter()
            .map(|a| a.create_array_builder(capacity))
            .collect();
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            children_array,
            type_: ty,
            len: 0,
        }
    }

    fn append_n(&mut self, n: usize, value: Option<StructRef<'_>>) {
        match value {
            None => {
                self.bitmap.append_n(n, false);
                for child in &mut self.children_array {
                    child.append_datum_n(n, Datum::None);
                }
            }
            Some(v) => {
                self.bitmap.append_n(n, true);
                iter_fields_ref!(v, fields, {
                    for (child, f) in self.children_array.iter_mut().zip_eq_fast(fields) {
                        child.append_datum_n(n, f);
                    }
                });
            }
        }
        self.len += n;
    }

    fn append_array(&mut self, other: &StructArray) {
        self.bitmap.append_bitmap(&other.bitmap);
        for (a, o) in self.children_array.iter_mut().zip_eq_fast(&other.children) {
            a.append_array(o);
        }
        self.len += other.len();
    }

    fn pop(&mut self) -> Option<()> {
        if self.bitmap.pop().is_some() {
            for child in &mut self.children_array {
                child.pop().unwrap()
            }
            self.len -= 1;

            Some(())
        } else {
            None
        }
    }

    fn finish(self) -> StructArray {
        let children = self
            .children_array
            .into_iter()
            .map(|b| Arc::new(b.finish()))
            .collect::<Vec<ArrayRef>>();
        StructArray::new(self.bitmap.finish(), children, self.type_.clone(), self.len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructArray {
    // TODO: the same bitmap is also stored in each child array, which is redundant.
    bitmap: Bitmap,
    children: Vec<ArrayRef>,
    type_: Arc<StructType>,
    len: usize,

    heap_size: usize,
}

impl StructArrayBuilder {
    pub fn append_array_refs(&mut self, refs: Vec<ArrayRef>, len: usize) {
        for _ in 0..len {
            self.bitmap.append(true);
        }
        self.len += len;
        for (a, r) in self.children_array.iter_mut().zip_eq_fast(refs.iter()) {
            a.append_array(r);
        }
    }
}

impl Array for StructArray {
    type Builder = StructArrayBuilder;
    type OwnedItem = StructValue;
    type RefItem<'a> = StructRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> StructRef<'_> {
        StructRef::Indexed { arr: self, idx }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn to_protobuf(&self) -> PbArray {
        let children_array = self.children.iter().map(|a| a.to_protobuf()).collect();
        let children_type = self.type_.fields.iter().map(|t| t.to_protobuf()).collect();
        PbArray {
            array_type: PbArrayType::Struct as i32,
            struct_array_data: Some(StructArrayData {
                children_array,
                children_type,
            }),
            list_array_data: None,
            null_bitmap: Some(self.bitmap.to_protobuf()),
            values: vec![],
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    fn data_type(&self) -> DataType {
        DataType::Struct(self.type_.clone())
    }
}

impl StructArray {
    fn new(bitmap: Bitmap, children: Vec<ArrayRef>, type_: Arc<StructType>, len: usize) -> Self {
        let heap_size = bitmap.estimated_heap_size()
            + children
                .iter()
                .map(|c| c.estimated_heap_size())
                .sum::<usize>();

        Self {
            bitmap,
            children,
            type_,
            len,
            heap_size,
        }
    }

    pub fn from_protobuf(array: &PbArray) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.values.is_empty(),
            "Must have no buffer in a struct array"
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        let cardinality = bitmap.len();
        let array_data = array.get_struct_array_data()?;
        let children = array_data
            .children_array
            .iter()
            .map(|child| Ok(Arc::new(ArrayImpl::from_protobuf(child, cardinality)?)))
            .collect::<ArrayResult<Vec<ArrayRef>>>()?;
        let type_ = Arc::new(StructType::unnamed(
            array_data
                .children_type
                .iter()
                .map(DataType::from)
                .collect(),
        ));
        let arr = Self::new(bitmap, children, type_, cardinality);
        Ok(arr.into())
    }

    pub fn children_array_types(&self) -> &[DataType] {
        &self.type_.fields
    }

    /// Returns an iterator over the field array.
    pub fn fields(&self) -> impl ExactSizeIterator<Item = &ArrayImpl> {
        self.children.iter().map(|f| &(**f))
    }

    pub fn field_at(&self, index: usize) -> ArrayRef {
        self.children[index].clone()
    }

    pub fn children_names(&self) -> &[String] {
        &self.type_.field_names
    }

    pub fn from_slices(
        null_bitmap: &[bool],
        children: Vec<ArrayImpl>,
        children_type: Vec<DataType>,
    ) -> StructArray {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::from_iter(null_bitmap.to_vec());
        let children = children.into_iter().map(Arc::new).collect();
        Self::new(
            bitmap,
            children,
            Arc::new(StructType::unnamed(children_type)),
            cardinality,
        )
    }

    pub fn from_slices_with_field_names(
        null_bitmap: &[bool],
        children: Vec<ArrayImpl>,
        children_type: Vec<DataType>,
        children_name: Vec<String>,
    ) -> StructArray {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::from_iter(null_bitmap.to_vec());
        let children = children.into_iter().map(Arc::new).collect_vec();
        let type_ = Arc::new(StructType {
            fields: children_type,
            field_names: children_name,
        });
        Self::new(bitmap, children, type_, cardinality)
    }

    #[cfg(test)]
    pub fn values_vec(&self) -> Vec<Option<StructValue>> {
        use crate::types::ScalarRef;

        self.iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec()
    }
}

impl EstimateSize for StructArray {
    fn estimated_heap_size(&self) -> usize {
        self.heap_size
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Hash)]
pub struct StructValue {
    fields: Box<[Datum]>,
}

impl PartialOrd for StructValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_scalar_ref().partial_cmp(&other.as_scalar_ref())
    }
}

impl Ord for StructValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl EstimateSize for StructValue {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Try speed up this process.
        self.fields
            .iter()
            .map(|datum| datum.estimated_heap_size())
            .sum()
    }
}

impl StructValue {
    pub fn new(fields: Vec<Datum>) -> Self {
        Self {
            fields: fields.into_boxed_slice(),
        }
    }

    pub fn fields(&self) -> &[Datum] {
        &self.fields
    }

    pub fn memcmp_deserialize(
        fields: &[DataType],
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        fields
            .iter()
            .map(|field| memcmp_encoding::deserialize_datum_in_composite(field, deserializer))
            .try_collect()
            .map(Self::new)
    }
}

#[derive(Copy, Clone)]
pub enum StructRef<'a> {
    Indexed { arr: &'a StructArray, idx: usize },
    ValueRef { val: &'a StructValue },
}

impl<'a> StructRef<'a> {
    /// Iterates over the fields of the struct.
    ///
    /// Prefer using the macro `iter_fields_ref!` if possible to avoid the cost of enum dispatching.
    pub fn iter_fields_ref(self) -> impl ExactSizeIterator<Item = DatumRef<'a>> + 'a {
        iter_fields_ref!(self, it, { Either::Left(it) }, { Either::Right(it) })
    }

    pub fn memcmp_serialize(
        self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        iter_fields_ref!(self, it, {
            for datum_ref in it {
                memcmp_encoding::serialize_datum_in_composite(datum_ref, serializer)?
            }
            Ok(())
        })
    }

    pub fn hash_scalar_inner<H: std::hash::Hasher>(self, state: &mut H) {
        iter_fields_ref!(self, it, {
            for datum_ref in it {
                hash_datum(datum_ref, state);
            }
        })
    }

    pub fn estimate_serialize_size_inner(self) -> usize {
        iter_fields_ref!(self, it, {
            it.fold(0, |acc, datum_ref| {
                acc + estimate_serialize_datum_size(datum_ref)
            })
        })
    }
}

impl PartialEq for StructRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        iter_fields_ref!(*self, lhs, {
            iter_fields_ref!(*other, rhs, { lhs.eq(rhs) })
        })
    }
}

impl PartialOrd for StructRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        iter_fields_ref!(*self, l, {
            iter_fields_ref!(*other, r, {
                if l.len() != r.len() {
                    return None;
                }
                Some(l.cmp_by(r, cmp_struct_field))
            })
        })
    }
}

// TODO(): use compare_datum
fn cmp_struct_field(l: impl ToDatumRef, r: impl ToDatumRef) -> Ordering {
    match (l.to_datum_ref(), r.to_datum_ref()) {
        // Comparability check was performed by frontend beforehand.
        (DatumRef::Some(sl), DatumRef::Some(sr)) => sl.partial_cmp(&sr).unwrap(),
        // Nulls are larger than everything, (1, null) > (1, 2) for example.
        (DatumRef::Some(_), DatumRef::None) => Ordering::Less,
        (DatumRef::None, DatumRef::Some(_)) => Ordering::Greater,
        (DatumRef::None, DatumRef::None) => Ordering::Equal,
    }
}

impl Debug for StructRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut is_first = true;
        iter_fields_ref!(*self, it, {
            for v in it {
                if is_first {
                    write!(f, "{:?}", v)?;
                    is_first = false;
                } else {
                    write!(f, ", {:?}", v)?;
                }
            }
            Ok(())
        })
    }
}

impl ToText for StructRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        iter_fields_ref!(*self, it, {
            write!(f, "(")?;
            let mut is_first = true;
            for x in it {
                if is_first {
                    is_first = false;
                } else {
                    write!(f, ",")?;
                }
                ToText::write(&x, f)?;
            }
            write!(f, ")")
        })
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            DataType::Struct(_) => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl Eq for StructRef<'_> {}

impl Ord for StructRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order between two structs is deterministic.
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use more_asserts::assert_gt;

    use super::*;
    use crate::{array, try_match_expand};

    // Empty struct is allowed in postgres.
    // `CREATE TYPE foo_empty as ();`, e.g.
    #[test]
    fn test_struct_new_empty() {
        let arr = StructArray::from_slices(&[true, false, true, false], vec![], vec![]);
        let actual = StructArray::from_protobuf(&arr.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);
    }

    #[test]
    fn test_struct_with_fields() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[false, true, false, true],
            vec![
                array! { I32Array, [None, Some(1), None, Some(2)] }.into(),
                array! { F32Array, [None, Some(3.0), None, Some(4.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        );
        let actual = StructArray::from_protobuf(&arr.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);

        let arr = try_match_expand!(actual, ArrayImpl::Struct).unwrap();
        let struct_values = arr.values_vec();
        assert_eq!(
            struct_values,
            vec![
                None,
                Some(StructValue::new(vec![1i32.into(), 3.0f32.into()])),
                None,
                Some(StructValue::new(vec![2i32.into(), 4.0f32.into()])),
            ]
        );

        let mut builder = StructArrayBuilder::with_type(
            4,
            DataType::Struct(Arc::new(StructType::unnamed(vec![
                DataType::Int32,
                DataType::Float32,
            ]))),
        );
        for v in &struct_values {
            builder.append(v.as_ref().map(|s| s.as_scalar_ref()));
        }
        let arr = builder.finish();
        assert_eq!(arr.values_vec(), struct_values);
    }

    // Ensure `create_builder` exactly copies the same metadata.
    #[test]
    fn test_struct_create_builder() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1)] }.into(),
                array! { F32Array, [Some(2.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        );
        let builder = arr.create_builder(4);
        let arr2 = builder.finish();
        assert_eq!(arr.data_type(), arr2.data_type());
    }

    #[test]
    fn test_struct_value_cmp() {
        // (1, 2.0) > (1, 1.0)
        assert_gt!(
            StructValue::new(vec![1.into(), 2.0.into()]),
            StructValue::new(vec![1.into(), 1.0.into()]),
        );
        // null > 1
        assert_eq!(
            cmp_struct_field(Datum::None, Datum::Some(1i32.into())),
            Ordering::Greater
        );
        // (1, null, 3) > (1, 1.0, 2)
        assert_gt!(
            StructValue::new(vec![1.into(), Datum::None, 3.into()]),
            StructValue::new(vec![1.into(), 1.0.into(), 2.into()]),
        );
        // (1, null) == (1, null)
        assert_eq!(
            StructValue::new(vec![1.into(), Datum::None]),
            StructValue::new(vec![1.into(), Datum::None]),
        );
    }

    #[test]
    fn test_serialize_deserialize() {
        let value = StructValue::new(vec![
            3.2f32.into(),
            "abcde".into(),
            StructValue::new(vec![
                1.3f64.into(),
                "a".into(),
                Datum::None,
                StructValue::new(vec![]).into(),
            ])
            .into(),
            Datum::None,
            "".into(),
            Datum::None,
            StructValue::new(vec![]).into(),
            12345.into(),
        ]);
        let fields = [
            DataType::Float32,
            DataType::Varchar,
            DataType::new_struct(
                vec![
                    DataType::Float64,
                    DataType::Varchar,
                    DataType::Varchar,
                    DataType::new_struct(vec![], vec![]),
                ],
                vec![],
            ),
            DataType::Int64,
            DataType::Varchar,
            DataType::Int16,
            DataType::new_struct(vec![], vec![]),
            DataType::Int32,
        ];
        let struct_ref = StructRef::ValueRef { val: &value };
        let mut serializer = memcomparable::Serializer::new(vec![]);
        struct_ref.memcmp_serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            StructValue::memcmp_deserialize(&fields, &mut deserializer).unwrap(),
            value
        );

        let mut builder = StructArrayBuilder::with_type(
            0,
            DataType::Struct(Arc::new(StructType::unnamed(fields.to_vec()))),
        );

        builder.append(Some(struct_ref));
        let array = builder.finish();
        let struct_ref = array.value_at(0).unwrap();
        let mut serializer = memcomparable::Serializer::new(vec![]);
        struct_ref.memcmp_serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            StructValue::memcmp_deserialize(&fields, &mut deserializer).unwrap(),
            value
        );
    }

    #[test]
    fn test_memcomparable() {
        let cases = [
            (
                StructValue::new(vec![123.into(), 456i64.into()]),
                StructValue::new(vec![123.into(), 789i64.into()]),
                vec![DataType::Int32, DataType::Int64],
                Ordering::Less,
            ),
            (
                StructValue::new(vec![123.into(), 456i64.into()]),
                StructValue::new(vec![1.into(), 789i64.into()]),
                vec![DataType::Int32, DataType::Int64],
                Ordering::Greater,
            ),
            (
                StructValue::new(vec!["".into()]),
                StructValue::new(vec![Datum::None]),
                vec![DataType::Varchar],
                Ordering::Less,
            ),
            (
                StructValue::new(vec!["abcd".into(), Datum::None]),
                StructValue::new(vec![
                    "abcd".into(),
                    StructValue::new(vec!["abcdef".into()]).into(),
                ]),
                vec![
                    DataType::Varchar,
                    DataType::new_struct(vec![DataType::Varchar], vec![]),
                ],
                Ordering::Greater,
            ),
            (
                StructValue::new(vec![
                    "abcd".into(),
                    StructValue::new(vec!["abcdef".into()]).into(),
                ]),
                StructValue::new(vec![
                    "abcd".into(),
                    StructValue::new(vec!["abcdef".into()]).into(),
                ]),
                vec![
                    DataType::Varchar,
                    DataType::new_struct(vec![DataType::Varchar], vec![]),
                ],
                Ordering::Equal,
            ),
        ];

        for (lhs, rhs, fields, order) in cases {
            let lhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                StructRef::ValueRef { val: &lhs }
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                StructRef::ValueRef { val: &rhs }
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), order);

            let mut builder = StructArrayBuilder::with_type(
                0,
                DataType::Struct(Arc::new(StructType::unnamed(fields.to_vec()))),
            );
            builder.append(Some(StructRef::ValueRef { val: &lhs }));
            builder.append(Some(StructRef::ValueRef { val: &rhs }));
            let array = builder.finish();
            let lhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                array
                    .value_at(0)
                    .unwrap()
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                array
                    .value_at(1)
                    .unwrap()
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), order);
        }
    }
}
