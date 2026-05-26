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
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geo::BooleanOps;
use geo_types::{Geometry, MultiPolygon};

use super::wkb_util::{geom_to_wkb, read_wkb, wkb_to_geo, as_binary_array};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StSymDifference {
    signature: Signature,
}

impl Default for StSymDifference {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::LargeBinary]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::LargeBinary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Binary]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StSymDifference {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_symdifference" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col1 = as_binary_array(&arrays[0])?;
        let col2 = as_binary_array(&arrays[1])?;

        let mut builder = BinaryBuilder::with_capacity(col1.len(), col1.len() * 128);
        for (b1, b2) in col1.iter().zip(col2.iter()) {
            match (b1, b2) {
                (Some(bytes1), Some(bytes2)) => {
                    let result = (|| -> Option<Vec<u8>> {
                        let a = as_multipolygon(wkb_to_geo(read_wkb(bytes1).ok()?).ok()?)?;
                        let b = as_multipolygon(wkb_to_geo(read_wkb(bytes2).ok()?).ok()?)?;
                        geom_to_wkb(&Geometry::MultiPolygon(a.xor(&b))).ok()
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

fn as_multipolygon(g: Geometry) -> Option<MultiPolygon<f64>> {
    match g {
        Geometry::Polygon(p) => Some(MultiPolygon(vec![p])),
        Geometry::MultiPolygon(mp) => Some(mp),
        _ => None,
    }
}
