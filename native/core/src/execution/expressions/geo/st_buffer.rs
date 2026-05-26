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

use arrow::array::{Array, ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geo::algorithm::buffer::{Buffer, BufferStyle};
use geo_types::Geometry;

use super::wkb_util::{geom_to_wkb, read_wkb, wkb_to_geo, as_binary_array};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StBuffer {
    signature: Signature,
}

impl Default for StBuffer {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Float64]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StBuffer {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_buffer" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        // SedonaDB pattern (sedona-db/rust/sedona-geo/src/st_buffer.rs):
        // cast distance arg to Float64 scalar BEFORE looping — handles literal scalars
        let dist_casted = args.args[1].cast_to(&DataType::Float64, None)?;
        let params: Option<BufferStyle<f64>> = if let ColumnarValue::Scalar(ref sv) = dist_casted {
            if sv.is_null() {
                None
            } else {
                Some(BufferStyle::new(f64::try_from(sv.clone()).map_err(|e| DataFusionError::External(Box::new(e)))?))
            }
        } else {
            return Err(DataFusionError::Execution("st_buffer: distance must be a scalar".into()));
        };

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geom_col = as_binary_array(&arrays[0])?;

        let mut builder = BinaryBuilder::with_capacity(geom_col.len(), geom_col.len() * 128);
        for b in geom_col.iter() {
            match (b, params.clone()) {
                (Some(bytes), Some(style)) => {
                    let result = (|| -> Option<Vec<u8>> {
                        let g = wkb_to_geo(read_wkb(bytes).ok()?).ok()?;
                        let buffered = g.buffer_with_style(style);
                        geom_to_wkb(&Geometry::MultiPolygon(buffered)).ok()
                    })();
                    match result {
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
