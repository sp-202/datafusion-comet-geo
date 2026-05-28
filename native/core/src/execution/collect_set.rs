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

//! Comet-native collect_set backed by a proper GroupsAccumulator.
//!
//! Implements GroupsAccumulator directly so DataFusion never wraps us in
//! GroupsAccumulatorAdapter. That adapter calls update_batch for both Partial
//! and Final phases, which breaks DistinctArrayAggAccumulator because the Final
//! phase passes List<T> state but update_batch expects raw T elements.
//!
//! With a real GroupsAccumulator the split is explicit:
//!   update_batch = Partial phase, receives raw T values
//!   merge_batch  = Final phase, receives List<T> state

use arrow::array::{Array, ArrayRef, BooleanArray, ListArray, new_empty_array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::EmitTo;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility};
use std::collections::HashSet;
use std::{any::Any, sync::Arc};

/// Comet's collect_set aggregate.
/// - Ignores NULL inputs (Spark semantics).
/// - Returns empty list instead of NULL when all inputs are NULL.
/// - Backed by a real GroupsAccumulator - no GroupsAccumulatorAdapter involved.
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

    /// Element type T from the input field.
    /// Partial mode: expr_fields[0] is T.
    /// PartialMerge/Final mode: expr_fields[0] is List<T> (the state column).
    /// Always returns T.
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

    /// Scalar accumulator for the no-GROUP-BY case.
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let element_type = Self::element_type(acc_args.expr_fields[0].data_type());
        Ok(Box::new(ScalarCollectSetAccumulator::new(element_type)))
    }

    /// Opt in to the GroupsAccumulator fast path.
    /// This prevents DataFusion from wrapping us in GroupsAccumulatorAdapter.
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let element_type = Self::element_type(args.expr_fields[0].data_type());
        Ok(Box::new(CollectSetGroupsAccumulator::new(element_type)))
    }
}

// ---------------------------------------------------------------------------
// Scalar accumulator (no GROUP BY)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ScalarCollectSetAccumulator {
    values: HashSet<ScalarValue>,
    element_type: DataType,
}

impl ScalarCollectSetAccumulator {
    fn new(element_type: DataType) -> Self {
        Self {
            values: HashSet::new(),
            element_type,
        }
    }
}

impl Accumulator for ScalarCollectSetAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for i in 0..values[0].len() {
            if values[0].is_valid(i) {
                self.values.insert(ScalarValue::try_from_array(&values[0], i)?);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("collect_set merge state must be a ListArray");
        for row in list.iter().flatten() {
            for i in 0..row.len() {
                if row.is_valid(i) {
                    self.values.insert(ScalarValue::try_from_array(&row, i)?);
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            let empty = new_empty_array(&self.element_type);
            return Ok(SingleRowListArrayBuilder::new(empty).build_list_scalar());
        }
        let vals: Vec<ScalarValue> = self.values.iter().cloned().collect();
        ScalarValue::new_list_nullable(&vals, &self.element_type)
    }

    fn size(&self) -> usize {
        self.values.iter().map(|v| v.size()).sum::<usize>() + self.element_type.size()
    }
}

// ---------------------------------------------------------------------------
// GroupsAccumulator (GROUP BY path) — one HashSet<ScalarValue> per group
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CollectSetGroupsAccumulator {
    /// Per-group distinct value sets.
    groups: Vec<HashSet<ScalarValue>>,
    element_type: DataType,
}

impl CollectSetGroupsAccumulator {
    fn new(element_type: DataType) -> Self {
        Self {
            groups: Vec::new(),
            element_type,
        }
    }

    fn ensure_groups(&mut self, total: usize) {
        while self.groups.len() < total {
            self.groups.push(HashSet::new());
        }
    }

    /// Build a ListArray from the first `n` groups, then drain them.
    fn emit(&mut self, n: usize) -> Result<ArrayRef> {
        let element_field = Arc::new(Field::new_list_field(self.element_type.clone(), true));

        // Build flat values array and offsets.
        let mut all_scalars: Vec<ScalarValue> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
        offsets.push(0);

        for group in self.groups.iter().take(n) {
            let start = all_scalars.len();
            all_scalars.extend(group.iter().cloned());
            offsets.push(all_scalars.len() as i32);
        }

        let flat_array = if all_scalars.is_empty() {
            new_empty_array(&self.element_type)
        } else {
            ScalarValue::iter_to_array(all_scalars.into_iter())?
        };

        let list = ListArray::new(
            element_field,
            OffsetBuffer::new(offsets.into()),
            flat_array,
            None, // no nulls - Spark returns empty list, not NULL
        );

        Ok(Arc::new(list))
    }
}

impl GroupsAccumulator for CollectSetGroupsAccumulator {
    /// Partial phase: receives raw T values, one per row.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_groups(total_num_groups);
        let input = &values[0];

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if let Some(f) = opt_filter {
                if f.is_null(row_idx) || !f.value(row_idx) {
                    continue;
                }
            }
            if input.is_null(row_idx) {
                continue; // Spark collect_set ignores NULLs
            }
            let sv = ScalarValue::try_from_array(input, row_idx)?;
            self.groups[group_idx].insert(sv);
        }
        Ok(())
    }

    /// Final/PartialMerge phase: receives List<T> state from partial agg.
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_groups(total_num_groups);
        let list = values[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("collect_set merge state must be ListArray<T>");

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if list.is_null(row_idx) {
                continue;
            }
            let row = list.value(row_idx);
            for i in 0..row.len() {
                if row.is_valid(i) {
                    let sv = ScalarValue::try_from_array(&row, i)?;
                    self.groups[group_idx].insert(sv);
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let n = match emit_to {
            EmitTo::All => self.groups.len(),
            EmitTo::First(n) => n,
        };
        let result = self.emit(n)?;
        match emit_to {
            EmitTo::All => self.groups.clear(),
            EmitTo::First(n) => { self.groups.drain(0..n); }
        }
        Ok(result)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // State is identical to evaluate output - ListArray of distinct values per group.
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn size(&self) -> usize {
        self.groups
            .iter()
            .map(|g| g.iter().map(|v| v.size()).sum::<usize>())
            .sum::<usize>()
            + self.element_type.size()
    }
}
