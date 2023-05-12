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

use std::cmp::{Ord, Ordering};
use std::fmt;
use std::sync::Arc;

use parse_display::Display;
use risingwave_pb::common::{PbColumnOrder, PbDirection, PbNullsAre, PbOrderType};

use crate::array::{Array, ArrayImpl, DataChunk};
use crate::catalog::{FieldDisplay, Schema};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::types::ToDatumRef;

/// Sort direction, ascending/descending.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Display, Default)]
enum Direction {
    #[default]
    #[display("ASC")]
    Ascending,
    #[display("DESC")]
    Descending,
}

impl Direction {
    pub fn from_protobuf(direction: &PbDirection) -> Self {
        match direction {
            PbDirection::Ascending => Self::Ascending,
            PbDirection::Descending => Self::Descending,
            PbDirection::Unspecified => unreachable!(),
        }
    }

    pub fn to_protobuf(self) -> PbDirection {
        match self {
            Self::Ascending => PbDirection::Ascending,
            Self::Descending => PbDirection::Descending,
        }
    }
}

/// Nulls are largest/smallest.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Display, Default)]
enum NullsAre {
    #[default]
    #[display("LARGEST")]
    Largest,
    #[display("SMALLEST")]
    Smallest,
}

impl NullsAre {
    pub fn from_protobuf(nulls_are: &PbNullsAre) -> Self {
        match nulls_are {
            PbNullsAre::Largest => Self::Largest,
            PbNullsAre::Smallest => Self::Smallest,
            PbNullsAre::Unspecified => unreachable!(),
        }
    }

    pub fn to_protobuf(self) -> PbNullsAre {
        match self {
            Self::Largest => PbNullsAre::Largest,
            Self::Smallest => PbNullsAre::Smallest,
        }
    }
}

/// Order type of a column.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Default)]
pub struct OrderType {
    direction: Direction,
    nulls_are: NullsAre,
}

impl OrderType {
    pub fn from_protobuf(order_type: &PbOrderType) -> OrderType {
        OrderType {
            direction: Direction::from_protobuf(&order_type.direction()),
            nulls_are: NullsAre::from_protobuf(&order_type.nulls_are()),
        }
    }

    pub fn to_protobuf(self) -> PbOrderType {
        PbOrderType {
            direction: self.direction.to_protobuf() as _,
            nulls_are: self.nulls_are.to_protobuf() as _,
        }
    }
}

impl OrderType {
    fn new(direction: Direction, nulls_are: NullsAre) -> Self {
        Self {
            direction,
            nulls_are,
        }
    }

    fn nulls_first(direction: Direction) -> Self {
        match direction {
            Direction::Ascending => Self::new(direction, NullsAre::Smallest),
            Direction::Descending => Self::new(direction, NullsAre::Largest),
        }
    }

    fn nulls_last(direction: Direction) -> Self {
        match direction {
            Direction::Ascending => Self::new(direction, NullsAre::Largest),
            Direction::Descending => Self::new(direction, NullsAre::Smallest),
        }
    }

    pub fn from_bools(asc: Option<bool>, nulls_first: Option<bool>) -> Self {
        let direction = match asc {
            None => Direction::default(),
            Some(true) => Direction::Ascending,
            Some(false) => Direction::Descending,
        };
        match nulls_first {
            None => Self::new(direction, NullsAre::default()),
            Some(true) => Self::nulls_first(direction),
            Some(false) => Self::nulls_last(direction),
        }
    }

    // TODO(rc): Many places that call `ascending` should've call `default`.
    /// Create an `ASC` order type.
    pub fn ascending() -> Self {
        Self {
            direction: Direction::Ascending,
            nulls_are: NullsAre::default(),
        }
    }

    /// Create a `DESC` order type.
    pub fn descending() -> Self {
        Self {
            direction: Direction::Descending,
            nulls_are: NullsAre::default(),
        }
    }

    /// Create an `ASC NULLS FIRST` order type.
    pub fn ascending_nulls_first() -> Self {
        Self::nulls_first(Direction::Ascending)
    }

    /// Create an `ASC NULLS LAST` order type.
    pub fn ascending_nulls_last() -> Self {
        Self::nulls_last(Direction::Ascending)
    }

    /// Create a `DESC NULLS FIRST` order type.
    pub fn descending_nulls_first() -> Self {
        Self::nulls_first(Direction::Descending)
    }

    /// Create a `DESC NULLS LAST` order type.
    pub fn descending_nulls_last() -> Self {
        Self::nulls_last(Direction::Descending)
    }

    pub fn is_ascending(&self) -> bool {
        self.direction == Direction::Ascending
    }

    pub fn is_descending(&self) -> bool {
        self.direction == Direction::Descending
    }

    pub fn nulls_are_largest(&self) -> bool {
        self.nulls_are == NullsAre::Largest
    }

    pub fn nulls_are_smallest(&self) -> bool {
        self.nulls_are == NullsAre::Smallest
    }

    pub fn nulls_are_first(&self) -> bool {
        self.is_ascending() && self.nulls_are_smallest()
            || self.is_descending() && self.nulls_are_largest()
    }

    pub fn nulls_are_last(&self) -> bool {
        !self.nulls_are_first()
    }
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.direction)?;
        if self.nulls_are != NullsAre::default() {
            write!(
                f,
                " NULLS {}",
                if self.nulls_are_first() {
                    "FIRST"
                } else {
                    "LAST"
                }
            )?;
        }
        Ok(())
    }
}

/// Column index with an order type (ASC or DESC). Used to represent a sort key
/// (`Vec<ColumnOrder>`).
///
/// Corresponds to protobuf [`PbColumnOrder`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ColumnOrder {
    pub column_index: usize,
    pub order_type: OrderType,
}

impl ColumnOrder {
    pub fn new(column_index: usize, order_type: OrderType) -> Self {
        Self {
            column_index,
            order_type,
        }
    }

    /// Shift the column index with offset.
    pub fn shift_with_offset(&mut self, offset: isize) {
        self.column_index = (self.column_index as isize + offset) as usize;
    }
}

impl ColumnOrder {
    pub fn from_protobuf(column_order: &PbColumnOrder) -> Self {
        ColumnOrder {
            column_index: column_order.column_index as _,
            order_type: OrderType::from_protobuf(column_order.get_order_type().unwrap()),
        }
    }

    pub fn to_protobuf(&self) -> PbColumnOrder {
        PbColumnOrder {
            column_index: self.column_index as _,
            order_type: Some(self.order_type.to_protobuf()),
        }
    }
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${} {}", self.column_index, self.order_type)
    }
}

impl fmt::Debug for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct ColumnOrderDisplay<'a> {
    pub column_order: &'a ColumnOrder,
    pub input_schema: &'a Schema,
}

impl fmt::Display for ColumnOrderDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.column_order;
        write!(
            f,
            "{} {}",
            FieldDisplay(self.input_schema.fields.get(that.column_index).unwrap()),
            that.order_type
        )
    }
}

#[derive(Clone, Debug)]
pub struct HeapElem {
    pub column_orders: Arc<Vec<ColumnOrder>>,
    pub chunk: DataChunk,
    pub chunk_idx: usize,
    pub elem_idx: usize,
    /// DataChunk can be encoded to accelerate the comparison.
    /// Use `risingwave_common::util::encoding_for_comparison::encode_chunk`
    /// to perform encoding, otherwise the comparison will be performed
    /// column by column.
    pub encoded_chunk: Option<Arc<Vec<Vec<u8>>>>,
}

impl Ord for HeapElem {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = if let (Some(lhs_encoded_chunk), Some(rhs_encoded_chunk)) =
            (self.encoded_chunk.as_ref(), other.encoded_chunk.as_ref())
        {
            lhs_encoded_chunk[self.elem_idx]
                .as_slice()
                .cmp(rhs_encoded_chunk[other.elem_idx].as_slice())
        } else {
            compare_rows_in_chunk(
                &self.chunk,
                self.elem_idx,
                &other.chunk,
                other.elem_idx,
                self.column_orders.as_ref(),
            )
            .unwrap()
        };
        ord.reverse()
    }
}

impl PartialOrd for HeapElem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapElem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapElem {}

fn compare_values<T>(lhs: Option<&T>, rhs: Option<&T>, order_type: OrderType) -> Ordering
where
    T: Ord,
{
    let ord = match (lhs, rhs, order_type.nulls_are) {
        (Some(l), Some(r), _) => l.cmp(r),
        (None, None, _) => Ordering::Equal,
        (Some(_), None, NullsAre::Largest) => Ordering::Less,
        (Some(_), None, NullsAre::Smallest) => Ordering::Greater,
        (None, Some(_), NullsAre::Largest) => Ordering::Greater,
        (None, Some(_), NullsAre::Smallest) => Ordering::Less,
    };
    if order_type.is_descending() {
        ord.reverse()
    } else {
        ord
    }
}

fn compare_values_in_array<'a, T>(
    lhs_array: &'a T,
    lhs_idx: usize,
    rhs_array: &'a T,
    rhs_idx: usize,
    order_type: OrderType,
) -> Ordering
where
    T: Array,
    <T as Array>::RefItem<'a>: Ord,
{
    compare_values(
        lhs_array.value_at(lhs_idx).as_ref(),
        rhs_array.value_at(rhs_idx).as_ref(),
        order_type,
    )
}

pub fn compare_rows_in_chunk(
    lhs_data_chunk: &DataChunk,
    lhs_idx: usize,
    rhs_data_chunk: &DataChunk,
    rhs_idx: usize,
    column_orders: &[ColumnOrder],
) -> Result<Ordering> {
    for column_order in column_orders.iter() {
        let lhs_array = lhs_data_chunk.column_at(column_order.column_index).array();
        let rhs_array = rhs_data_chunk.column_at(column_order.column_index).array();
        macro_rules! gen_match {
            ( $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
                match (lhs_array.as_ref(), rhs_array.as_ref()) {
                    $((ArrayImpl::$variant_name(lhs_inner), ArrayImpl::$variant_name(rhs_inner)) => Ok(compare_values_in_array(lhs_inner, lhs_idx, rhs_inner, rhs_idx, column_order.order_type)),)*
                    (l_arr, r_arr) => Err(InternalError(format!("Unmatched array types, lhs array is: {}, rhs array is: {}", l_arr.get_ident(), r_arr.get_ident()))),
                }?
            }
        }
        let res = for_all_variants! { gen_match };
        if res != Ordering::Equal {
            return Ok(res);
        }
    }
    Ok(Ordering::Equal)
}

/// Compare two `Datum`s with specified order type.
pub fn compare_datum(
    lhs: impl ToDatumRef,
    rhs: impl ToDatumRef,
    order_type: OrderType,
) -> Ordering {
    compare_values(
        lhs.to_datum_ref().as_option(),
        rhs.to_datum_ref().as_option(),
        order_type,
    )
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use itertools::Itertools;

    use super::*;
    use crate::array::{DataChunk, ListValue, StructValue};
    use crate::row::{OwnedRow, Row};
    use crate::types::{DataType, Datum, ScalarImpl};

    #[test]
    fn test_order_type() {
        assert_eq!(OrderType::default(), OrderType::ascending());
        assert_eq!(
            OrderType::default(),
            OrderType::new(Direction::Ascending, NullsAre::Largest)
        );
        assert_eq!(
            OrderType::default(),
            OrderType::from_bools(Some(true), Some(false))
        );
        assert_eq!(OrderType::default(), OrderType::from_bools(None, None));

        assert!(OrderType::ascending().is_ascending());
        assert!(OrderType::ascending().nulls_are_largest());
        assert!(OrderType::ascending().nulls_are_last());

        assert!(OrderType::descending().is_descending());
        assert!(OrderType::descending().nulls_are_largest());
        assert!(OrderType::descending().nulls_are_first());

        assert!(OrderType::ascending_nulls_first().is_ascending());
        assert!(OrderType::ascending_nulls_first().nulls_are_smallest());
        assert!(OrderType::ascending_nulls_first().nulls_are_first());

        assert!(OrderType::ascending_nulls_last().is_ascending());
        assert!(OrderType::ascending_nulls_last().nulls_are_largest());
        assert!(OrderType::ascending_nulls_last().nulls_are_last());

        assert!(OrderType::descending_nulls_first().is_descending());
        assert!(OrderType::descending_nulls_first().nulls_are_largest());
        assert!(OrderType::descending_nulls_first().nulls_are_first());

        assert!(OrderType::descending_nulls_last().is_descending());
        assert!(OrderType::descending_nulls_last().nulls_are_smallest());
        assert!(OrderType::descending_nulls_last().nulls_are_last());
    }

    #[test]
    fn test_compare_rows_in_chunk() {
        let v10 = Datum::Some(ScalarImpl::Int32(42i32));
        let v11 = Datum::Some(ScalarImpl::Utf8("hello".into()));
        let v12 = Datum::Some(ScalarImpl::Float32(4.0f32.into()));
        let v20 = Datum::Some(ScalarImpl::Int32(42i32));
        let v21 = Datum::Some(ScalarImpl::Utf8("hell".into()));
        let v22 = Datum::Some(ScalarImpl::Float32(3.0f32.into()));

        let row1 = OwnedRow::new(vec![v10, v11, v12]);
        let row2 = OwnedRow::new(vec![v20, v21, v22]);
        let chunk = DataChunk::from_rows(
            &[row1, row2],
            &[DataType::Int32, DataType::Varchar, DataType::Float32],
        );
        let column_orders = vec![
            ColumnOrder::new(0, OrderType::ascending()),
            ColumnOrder::new(1, OrderType::descending()),
        ];

        assert_eq!(
            Ordering::Equal,
            compare_rows_in_chunk(&chunk, 0, &chunk, 0, &column_orders).unwrap()
        );
        assert_eq!(
            Ordering::Less,
            compare_rows_in_chunk(&chunk, 0, &chunk, 1, &column_orders).unwrap()
        );
    }

    #[test]
    fn test_compare_all_types() {
        let row1 = OwnedRow::new(vec![
            Datum::Some(ScalarImpl::Int16(16)),
            Datum::Some(ScalarImpl::Int32(32)),
            Datum::Some(ScalarImpl::Int64(64)),
            Datum::Some(ScalarImpl::Float32(3.2.into())),
            Datum::Some(ScalarImpl::Float64(6.4.into())),
            Datum::Some(ScalarImpl::Utf8("hello".into())),
            Datum::Some(ScalarImpl::Bool(true)),
            Datum::Some(ScalarImpl::Decimal(10.into())),
            Datum::Some(ScalarImpl::Interval(Default::default())),
            Datum::Some(ScalarImpl::Date(Default::default())),
            Datum::Some(ScalarImpl::Timestamp(Default::default())),
            Datum::Some(ScalarImpl::Time(Default::default())),
            Datum::Some(ScalarImpl::Struct(StructValue::new(vec![
                Datum::Some(ScalarImpl::Int32(1)),
                Datum::Some(ScalarImpl::Float32(3.0.into())),
            ]))),
            Datum::Some(ScalarImpl::List(ListValue::new(vec![
                Datum::Some(ScalarImpl::Int32(1)),
                Datum::Some(ScalarImpl::Int32(2)),
            ]))),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Some(ScalarImpl::Int16(16)),
            Datum::Some(ScalarImpl::Int32(32)),
            Datum::Some(ScalarImpl::Int64(64)),
            Datum::Some(ScalarImpl::Float32(3.2.into())),
            Datum::Some(ScalarImpl::Float64(6.4.into())),
            Datum::Some(ScalarImpl::Utf8("hello".into())),
            Datum::Some(ScalarImpl::Bool(true)),
            Datum::Some(ScalarImpl::Decimal(10.into())),
            Datum::Some(ScalarImpl::Interval(Default::default())),
            Datum::Some(ScalarImpl::Date(Default::default())),
            Datum::Some(ScalarImpl::Timestamp(Default::default())),
            Datum::Some(ScalarImpl::Time(Default::default())),
            Datum::Some(ScalarImpl::Struct(StructValue::new(vec![
                Datum::Some(ScalarImpl::Int32(1)),
                Datum::Some(ScalarImpl::Float32(33333.0.into())), // larger than row1
            ]))),
            Datum::Some(ScalarImpl::List(ListValue::new(vec![
                Datum::Some(ScalarImpl::Int32(1)),
                Datum::Some(ScalarImpl::Int32(2)),
            ]))),
        ]);

        let column_orders = (0..row1.len())
            .map(|i| ColumnOrder::new(i, OrderType::ascending()))
            .collect_vec();

        let chunk = DataChunk::from_rows(
            &[row1, row2],
            &[
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Float64,
                DataType::Varchar,
                DataType::Boolean,
                DataType::Decimal,
                DataType::Interval,
                DataType::Date,
                DataType::Timestamp,
                DataType::Time,
                DataType::new_struct(vec![DataType::Int32, DataType::Float32], vec![]),
                DataType::List(Box::new(DataType::Int32)),
            ],
        );
        assert_eq!(
            Ordering::Equal,
            compare_rows_in_chunk(&chunk, 0, &chunk, 0, &column_orders).unwrap()
        );
        assert_eq!(
            Ordering::Less,
            compare_rows_in_chunk(&chunk, 0, &chunk, 1, &column_orders).unwrap()
        );
    }

    #[test]
    fn test_compare_datum() {
        assert_eq!(
            Ordering::Equal,
            compare_datum(
                Datum::Some(42.into()),
                Datum::Some(42.into()),
                OrderType::default(),
            )
        );
        assert_eq!(
            Ordering::Equal,
            compare_datum(Datum::None, Datum::None, OrderType::default(),)
        );
        assert_eq!(
            Ordering::Less,
            compare_datum(
                Datum::Some(42.into()),
                Datum::Some(100.into()),
                OrderType::ascending(),
            )
        );
        assert_eq!(
            Ordering::Greater,
            compare_datum(
                Datum::Some(42.into()),
                Datum::None,
                OrderType::ascending_nulls_first(),
            )
        );
        assert_eq!(
            Ordering::Less,
            compare_datum(
                Datum::Some(42.into()),
                Datum::None,
                OrderType::ascending_nulls_last(),
            )
        );
        assert_eq!(
            Ordering::Greater,
            compare_datum(
                Datum::Some(42.into()),
                Datum::None,
                OrderType::descending_nulls_first(),
            )
        );
        assert_eq!(
            Ordering::Less,
            compare_datum(
                Datum::Some(42.into()),
                Datum::None,
                OrderType::descending_nulls_last(),
            )
        );
    }
}
