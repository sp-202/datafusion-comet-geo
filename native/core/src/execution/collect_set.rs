// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Comet-local implementation of collect_set that correctly handles both Partial
//! and PartialMerge/Final aggregate modes.
//!
//! The upstream datafusion-spark SparkCollectSet passes acc_args.expr_fields[0]
//! directly to DistinctArrayAggAccumulator::try_new as the element type. In
//! Partial mode this is fine (the input is the raw element, e.g. Utf8). But in
//! PartialMerge/Final mode the input expression is the STATE column, which has
//! type List<Utf8> - the outer list wrapping the partial aggregate's output. Using
//! List<Utf8> as the element type makes the accumulator treat each state row as
//! a nested list and panics in merge_batch with "list array" when it tries to
//! as_list::<i32>() on a List<List<Utf8>> instead of List<Utf8>.
//!
//! Fix: unwrap one level of List when the input field is already a list type.
//! This correctly recovers the element type for the PartialMerge path.

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::array_agg::DistinctArrayAggAccumulator;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use std::{any::Any, sync::Arc};

/// Comet's collect_set aggregate function.
///
/// Differences from DataFusion ArrayAgg with distinct:
/// - Ignores NULL inputs (Spark semantics).
/// - Returns an empty list instead of NULL when all inputs are NULL.
/// - Correctly handles both Partial and PartialMerge/Final modes by
///   deriving the element type from the innermost list element, not the
///   raw expr_fields type which changes between planning passes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CometCollectSet {
    signature: Signature,
}

impl Default for CometCollectSet {
    fn default() -> Self {
        Self::new()
    }
}

impl CometCollectSet {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// Extract the true element type from the accumulator input field.
    ///
    /// In Partial mode:     expr_fields[0] has type T   (the raw input column).
    /// In PartialMerge mode: expr_fields[0] has type List<T> (the state column).
    ///
    /// We always want T - so unwrap one List level if present.
    fn element_type(input_data_type: &DataType) -> DataType {
        match input_data_type {
            DataType::List(inner_field) => inner_field.data_type().clone(),
            DataType::LargeList(inner_field) => inner_field.data_type().clone(),
            other => other.clone(),
        }
    }
}

impl AggregateUDFImpl for CometCollectSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "collect_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // arg_types[0] may be List<T> in PartialMerge - unwrap to get T.
        let element_type = Self::element_type(&arg_types[0]);
        Ok(DataType::List(Arc::new(Field::new_list_field(
            element_type,
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let element_type = Self::element_type(args.input_fields[0].data_type());
        Ok(vec![Field::new_list(
            format_state_name(args.name, "collect_set"),
            Field::new_list_field(element_type, true),
            true,
        )
        .into()])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input_type = acc_args.expr_fields[0].data_type().clone();
        let element_type = Self::element_type(&input_type);
        let ignore_nulls = true;
        Ok(Box::new(NullToEmptyListAccumulator::new(
            DistinctArrayAggAccumulator::try_new(&element_type, None, ignore_nulls)?,
            element_type,
        )))
    }
}

/// Wraps an inner Accumulator so that evaluate() returns an empty list
/// instead of NULL when all inputs were NULL (Spark semantics).
#[derive(Debug)]
struct NullToEmptyListAccumulator<T: Accumulator> {
    inner: T,
    element_type: DataType,
}

impl<T: Accumulator> NullToEmptyListAccumulator<T> {
    pub fn new(inner: T, element_type: DataType) -> Self {
        Self {
            inner,
            element_type,
        }
    }
}

impl<T: Accumulator> Accumulator for NullToEmptyListAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // In the Final/PartialMerge phase, GroupsAccumulatorAdapter calls update_batch
        // with the partial-aggregate state column (type List<element_type>) instead of
        // calling merge_batch. Detect this by checking whether the input array's type
        // matches the state type (List<element_type>) and route to merge_batch.
        if values.len() == 1 {
            let list_state_type =
                DataType::List(Arc::new(Field::new_list_field(self.element_type.clone(), true)));
            if values[0].data_type() == &list_state_type {
                return self.inner.merge_batch(values);
            }
        }
        self.inner.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.inner.evaluate()?;
        if result.is_null() {
            let empty_array = arrow::array::new_empty_array(&self.element_type);
            Ok(SingleRowListArrayBuilder::new(empty_array).build_list_scalar())
        } else {
            Ok(result)
        }
    }

    fn size(&self) -> usize {
        self.inner.size() + self.element_type.size()
    }
}
