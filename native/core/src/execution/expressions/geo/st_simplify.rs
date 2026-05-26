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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, Float64Array};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use geo::Simplify;

use super::wkb_util::{geom_to_wkb, read_wkb, wkb_to_geo};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StSimplify {
    signature: Signature,
}

impl Default for StSimplify {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary, DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StSimplify {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_simplify" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geom_col = arrays[0].as_any().downcast_ref::<BinaryArray>().unwrap();
        let tol_col = arrays[1].as_any().downcast_ref::<Float64Array>().unwrap();

        let mut builder = BinaryBuilder::with_capacity(geom_col.len(), geom_col.len() * 64);
        for (b, t) in geom_col.iter().zip(tol_col.iter()) {
            match (b, t) {
                (Some(bytes), Some(tol)) => {
                    match wkb_to_geo(read_wkb(bytes).ok()?).ok()
                        .and_then(|g| geom_to_wkb(&g.simplify(tol)).ok())
                    {
                        Some(wkb) => builder.append_value(&wkb),
                        None => builder.append_null(),
                    }
                }
                _ => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}
