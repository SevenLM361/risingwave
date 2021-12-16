//! Implementation of `StreamingFoldAgg`, which includes sum and count.

use std::marker::PhantomData;

use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::ErrorCode::{NotImplementedError, NumericValueOutOfRange};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{Datum, Scalar, ScalarRef};

use super::{StreamingAggFunction, StreamingAggState, StreamingAggStateImpl};

/// A trait over all fold functions.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
pub trait StreamingFoldable<R: Scalar, I: Scalar>: std::fmt::Debug + Send + Sync + 'static {
    /// Called on `Insert` or `UpdateInsert`.
    fn accumulate(result: Option<&R>, input: Option<I::ScalarRefType<'_>>) -> Result<Option<R>>;

    /// Called on `Delete` or `UpdateDelete`.
    fn retract(result: Option<&R>, input: Option<I::ScalarRefType<'_>>) -> Result<Option<R>>;

    /// Get initial value of this foldable function.
    fn initial() -> Option<R> {
        None
    }
}

/// `StreamingSumAgg` wraps a streaming summing function `S` into an aggregator.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
/// `S`: Sum function.
#[derive(Debug)]
pub struct StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    result: Option<R::OwnedItem>,
    _phantom: PhantomData<(I, S, R)>,
}

impl<R, I, S> Clone for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            _phantom: PhantomData,
        }
    }
}

/// `PrimitiveSummable` sums two primitives by `accumulate` and `retract` functions.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct PrimitiveSummable<S, I>
where
    I: Scalar + Into<S> + std::ops::Neg<Output = I>,
    S: Scalar + num_traits::CheckedAdd<Output = S> + num_traits::CheckedSub<Output = S>,
{
    _phantom: PhantomData<(S, I)>,
}

impl<S, I> StreamingFoldable<S, I> for PrimitiveSummable<S, I>
where
    I: Scalar + Into<S> + std::ops::Neg<Output = I>,
    S: Scalar + num_traits::CheckedAdd<Output = S> + num_traits::CheckedSub<Output = S>,
{
    fn accumulate(result: Option<&S>, input: Option<I::ScalarRefType<'_>>) -> Result<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(
                x.checked_add(&(y.to_owned_scalar()).into())
                    .ok_or_else(|| RwError::from(NumericValueOutOfRange))?,
            ),
            (Some(x), None) => Some(x.clone()),
            (None, Some(y)) => Some((y.to_owned_scalar()).into()),
            (None, None) => None,
        })
    }

    fn retract(result: Option<&S>, input: Option<I::ScalarRefType<'_>>) -> Result<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(
                x.checked_sub(&(y.to_owned_scalar()).into())
                    .ok_or_else(|| RwError::from(NumericValueOutOfRange))?,
            ),
            (Some(x), None) => Some(x.clone()),
            (None, Some(y)) => Some((-y.to_owned_scalar()).into()),
            (None, None) => None,
        })
    }
}

/// `Countable` do counts. The behavior of `Countable` is somehow counterintuitive.
/// In SQL logic, if there is no item in aggregation, count will return `null`.
/// However, this `Countable` will always return 0 if there is no item.
#[derive(Debug)]
pub struct Countable<S>
where
    S: Scalar,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<i64, S> for Countable<S>
where
    S: Scalar,
{
    fn accumulate(
        result: Option<&i64>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> Result<Option<i64>> {
        Ok(match (result, input) {
            (Some(x), Some(_)) => Some(x + 1),
            (Some(x), None) => Some(*x),
            // this is not possible, as initial value of countable is `0`.
            _ => unreachable!(),
        })
    }

    fn retract(result: Option<&i64>, input: Option<S::ScalarRefType<'_>>) -> Result<Option<i64>> {
        Ok(match (result, input) {
            (Some(x), Some(_)) => Some(x - 1),
            (Some(x), None) => Some(*x),
            // this is not possible, as initial value of countable is `0`.
            _ => unreachable!(),
        })
    }

    fn initial() -> Option<i64> {
        Some(0)
    }
}

/// `Minimizable` return minimum value overall.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct Minimizable<S>
where
    S: Scalar + Ord,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for Minimizable<S>
where
    S: Scalar + Ord,
{
    fn accumulate(result: Option<&S>, input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(x.clone().min(y.to_owned_scalar())),
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (Some(x), None) => Some(x.clone()),
            (None, None) => None,
        })
    }

    fn retract(_result: Option<&S>, _input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        Err(RwError::from(NotImplementedError(
            "insert only for minimum".to_string(),
        )))
    }
}

/// `FloatMinimizable` return minimum float value overall.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct FloatMinimizable<S>
where
    S: Scalar + num_traits::Float,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for FloatMinimizable<S>
where
    S: Scalar + num_traits::Float,
{
    fn accumulate(result: Option<&S>, input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        let valid = |val: S| -> bool { val.is_finite() && !val.is_nan() };

        Ok(match (result, input) {
            (Some(x), Some(y)) => {
                if valid(*x) && valid(y.to_owned_scalar()) {
                    Some((*x).min(y.to_owned_scalar()))
                } else {
                    return Err(RwError::from(NumericValueOutOfRange));
                }
            }
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (Some(x), None) => Some(*x),
            (None, None) => None,
        })
    }

    fn retract(_result: Option<&S>, _input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        Err(RwError::from(NotImplementedError(
            "insert only for float minimum".to_string(),
        )))
    }
}

/// `Maximizable` return maximum value overall.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct Maximizable<S>
where
    S: Scalar + Ord,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for Maximizable<S>
where
    S: Scalar + Ord,
{
    fn accumulate(result: Option<&S>, input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(x.clone().max(y.to_owned_scalar())),
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (Some(x), None) => Some(x.clone()),
            (None, None) => None,
        })
    }

    fn retract(_result: Option<&S>, _input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        Err(RwError::from(NotImplementedError(
            "insert only for maximum".to_string(),
        )))
    }
}

/// `FloatMaximizable` return maximum float value overall.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct FloatMaximizable<S>
where
    S: Scalar + num_traits::Float,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for FloatMaximizable<S>
where
    S: Scalar + num_traits::Float,
{
    fn accumulate(result: Option<&S>, input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        let valid = |val: S| -> bool { val.is_finite() && !val.is_nan() };

        Ok(match (result, input) {
            (Some(x), Some(y)) => {
                if valid(*x) && valid(y.to_owned_scalar()) {
                    Some((*x).max(y.to_owned_scalar()))
                } else {
                    return Err(RwError::from(NumericValueOutOfRange));
                }
            }
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (Some(x), None) => Some(*x),
            (None, None) => None,
        })
    }

    fn retract(_result: Option<&S>, _input: Option<S::ScalarRefType<'_>>) -> Result<Option<S>> {
        Err(RwError::from(NotImplementedError(
            "insert only for float maximum".to_string(),
        )))
    }
}

impl<R, I, S> StreamingAggState<I> for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &I,
    ) -> Result<()> {
        match visibility {
            None => {
                for (op, data) in ops.iter().zip(data.iter()) {
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            self.result = S::accumulate(self.result.as_ref(), data)?
                        }
                        Op::Delete | Op::UpdateDelete => {
                            self.result = S::retract(self.result.as_ref(), data)?
                        }
                    }
                }
            }
            Some(visibility) => {
                for ((visible, op), data) in visibility.iter().zip(ops.iter()).zip(data.iter()) {
                    if visible {
                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                self.result = S::accumulate(self.result.as_ref(), data)?
                            }
                            Op::Delete | Op::UpdateDelete => {
                                self.result = S::retract(self.result.as_ref(), data)?
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<R, I, S> StreamingAggFunction<R::Builder> for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn get_output_concrete(&self) -> Result<Option<R::OwnedItem>> {
        Ok(self.result.clone())
    }
}

impl<R, I, S> Default for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn default() -> Self {
        Self {
            result: S::initial(),
            _phantom: PhantomData,
        }
    }
}

impl<R, I, S> StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    pub fn new() -> Self {
        Self::default()
    }
    /// Get current state without using an array builder
    pub fn get_state(&self) -> &Option<R::OwnedItem> {
        &self.result
    }
}

impl<R, I, S> TryFrom<Datum> for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    type Error = RwError;

    fn try_from(x: Datum) -> Result<Self> {
        let mut result = None;
        if let Some(scalar) = x {
            result = Some(R::OwnedItem::try_from(scalar)?);
        }

        Ok(Self {
            result,
            _phantom: PhantomData,
        })
    }
}

macro_rules! impl_fold_agg {
    ($result:tt, $result_variant:tt, $input:tt) => {
        impl<S> StreamingAggStateImpl for StreamingFoldAgg<$result, $input, S>
        where
            S: StreamingFoldable<<$result as Array>::OwnedItem, <$input as Array>::OwnedItem>,
        {
            fn apply_batch(
                &mut self,
                ops: Ops<'_>,
                visibility: Option<&Bitmap>,
                data: &[&ArrayImpl],
            ) -> Result<()> {
                self.apply_batch_concrete(ops, visibility, data[0].into())
            }

            fn get_output(&self) -> Result<Datum> {
                Ok(self.result.map(Scalar::to_scalar_value))
            }

            fn new_builder(&self) -> ArrayBuilderImpl {
                ArrayBuilderImpl::$result_variant(<$result as Array>::Builder::new(0).unwrap())
            }
        }
    };
}

// Implement all supported combination of input and output for `StreamingFoldAgg`.
impl_fold_agg! { I16Array, Int16, I16Array }
impl_fold_agg! { I32Array, Int32, I32Array }
impl_fold_agg! { I64Array, Int64, I64Array }
impl_fold_agg! { F32Array, Float32, F32Array }
impl_fold_agg! { F64Array, Float64, F64Array }
impl_fold_agg! { F64Array, Float64, F32Array }
impl_fold_agg! { I64Array, Int64, F64Array }
impl_fold_agg! { I64Array, Int64, F32Array }
impl_fold_agg! { I64Array, Int64, I32Array }
impl_fold_agg! { I64Array, Int64, I16Array }
impl_fold_agg! { I64Array, Int64, BoolArray }
impl_fold_agg! { I64Array, Int64, Utf8Array }
impl_fold_agg! { I64Array, Int64, DecimalArray }
impl_fold_agg! { DecimalArray, Decimal, I64Array }
impl_fold_agg! { DecimalArray, Decimal, DecimalArray }

#[cfg(test)]
mod tests {
    use risingwave_common::array::I64Array;
    use risingwave_common::types::OrderedF64;
    use risingwave_common::{array, array_nonnull};

    use super::*;

    type TestStreamingSumAgg<R> =
        StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem, <R as Array>::OwnedItem>>;

    type TestStreamingCountAgg<R> = StreamingFoldAgg<R, R, Countable<<R as Array>::OwnedItem>>;

    type TestStreamingMinAgg<R> = StreamingFoldAgg<R, R, Minimizable<<R as Array>::OwnedItem>>;

    type TestStreamingFloatMinAgg<R> =
        StreamingFoldAgg<R, R, FloatMinimizable<<R as Array>::OwnedItem>>;

    type TestStreamingMaxAgg<R> = StreamingFoldAgg<R, R, Maximizable<<R as Array>::OwnedItem>>;

    #[test]
    /// This test uses `Box<dyn StreamingAggStateImpl>` to test a state.
    fn test_primitive_sum_boxed() {
        let mut agg: Box<dyn StreamingAggStateImpl> =
            Box::new(TestStreamingSumAgg::<I64Array>::new());
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 3, 3]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &3);

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Delete, Op::Insert],
            Some(&(vec![true, true, false, false]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &5);
    }

    #[test]
    fn test_primitive_sum_i64() {
        let mut agg = TestStreamingSumAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 3, 3]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &3);

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Delete, Op::Insert],
            Some(&(vec![true, true, false, false]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &5);
    }

    #[test]
    fn test_primitive_sum_f64() {
        let testcases = [
            (vec![('+', 1.0), ('+', 2.0), ('+', 3.0), ('-', 4.0)], 2.0),
            (
                vec![('+', 1.0), ('+', f64::INFINITY), ('+', 3.0), ('-', 3.0)],
                f64::INFINITY,
            ),
            (vec![('+', 0.0), ('-', f64::NEG_INFINITY)], f64::INFINITY),
            (vec![('+', 1.0), ('+', f64::NAN), ('+', 1926.0)], f64::NAN),
        ];

        for (input, expected) in testcases {
            let (ops, data): (Vec<_>, Vec<_>) = input
                .into_iter()
                .map(|(c, v)| {
                    (
                        if c == '+' { Op::Insert } else { Op::Delete },
                        Some(OrderedF64::from(v)),
                    )
                })
                .unzip();
            let mut agg = TestStreamingSumAgg::<F64Array>::new();
            agg.apply_batch(
                &ops,
                None,
                &[&ArrayImpl::Float64(F64Array::from_slice(&data).unwrap())],
            )
            .unwrap();
            assert_eq!(
                agg.get_output().unwrap().unwrap().as_float64(),
                &OrderedF64::from(expected)
            );
        }
    }

    #[test]
    fn test_primitive_sum_first_deletion() {
        let mut agg = TestStreamingSumAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Delete, Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [10, 1, 2, 3, 3]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &-7);

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![false, true, false, false]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &-8);
    }

    #[test]
    /// Even if there is no element after some insertions and equal number of deletion operations,
    /// `PrimitiveSummable` should output `0` instead of `None`.
    fn test_primitive_sum_no_none() {
        let mut agg = TestStreamingSumAgg::<I64Array>::new();

        assert_eq!(agg.get_output().unwrap(), None);

        agg.apply_batch(
            &[Op::Delete, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 1, 2]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Insert],
            Some(&(vec![false, true, false, true]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);
    }

    #[test]
    fn test_primitive_count() {
        let mut agg = TestStreamingCountAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array!(I64Array, [Some(1), None, Some(3), Some(1)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &1);

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![false, true, false, false]).try_into().unwrap()),
            &[&array!(I64Array, [Some(1), None, Some(3), Some(1)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &1);
    }

    #[test]
    fn test_minimum() {
        let mut agg = TestStreamingMinAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(1), Some(10), None, Some(5)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &1);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(1), Some(10), Some(-1), Some(5)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &-1);
    }

    #[test]
    fn test_minimum_float() {
        let mut agg = TestStreamingFloatMinAgg::<F64Array>::new();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(F64Array, [Some(1.0), Some(10.0), None, Some(5.0)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_float64(), &1.0);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(F64Array, [Some(1.0), Some(10.0), Some(-1.0), Some(5.0)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_float64(), &-1.0);
    }

    #[test]
    fn test_maximum() {
        let mut agg = TestStreamingMaxAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(10), Some(1), None, Some(5)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &10);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(1), Some(10), Some(100), Some(5)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &100);
    }
}
