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

use arrow::array::{Array, ArrayRef, BinaryBuilder, Decimal128Array, Float64Array};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use geo_types::{coord, Geometry, Polygon, LineString};

use super::wkb_util::geom_to_wkb;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StMakeEnvelope {
    signature: Signature,
}

impl Default for StMakeEnvelope {
    fn default() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StMakeEnvelope {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_makeenvelope" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let xmins = extract_f64_col(&arrays[0]);
        let ymins = extract_f64_col(&arrays[1]);
        let xmaxs = extract_f64_col(&arrays[2]);
        let ymaxs = extract_f64_col(&arrays[3]);

        let mut builder = BinaryBuilder::with_capacity(xmins.len(), xmins.len() * 100);
        for i in 0..xmins.len() {
            match (xmins[i], ymins[i], xmaxs[i], ymaxs[i]) {
                (Some(xmin), Some(ymin), Some(xmax), Some(ymax)) => {
                    let exterior = LineString(vec![
                        coord! { x: xmin, y: ymin },
                        coord! { x: xmax, y: ymin },
                        coord! { x: xmax, y: ymax },
                        coord! { x: xmin, y: ymax },
                        coord! { x: xmin, y: ymin },
                    ]);
                    let poly = Polygon::new(exterior, vec![]);
                    match geom_to_wkb(&Geometry::Polygon(poly)).ok() {
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

fn extract_f64_col(arr: &dyn Array) -> Vec<Option<f64>> {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.iter().collect();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match arr.data_type() {
            DataType::Decimal128(_, s) => *s as i32,
            _ => 0,
        };
        return a.iter().map(|v| v.map(|n| (n as f64) / 10f64.powi(scale))).collect();
    }
    vec![None; arr.len()]
}
