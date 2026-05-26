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

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Float64Array};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
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
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geom_col = as_binary_array(&arrays[0])?;
        let dist_col = arrays[1].as_any().downcast_ref::<Float64Array>().unwrap();

        let mut builder = BinaryBuilder::with_capacity(geom_col.len(), geom_col.len() * 128);
        for (b, d) in geom_col.iter().zip(dist_col.iter()) {
            match (b, d) {
                (Some(bytes), Some(dist)) => {
                    let result = (|| -> Option<Vec<u8>> {
                        let g = wkb_to_geo(read_wkb(bytes).ok()?).ok()?;
                        let style = BufferStyle::new(dist);
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
