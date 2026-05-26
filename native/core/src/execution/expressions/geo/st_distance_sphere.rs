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
use std::f64::consts::PI;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geo::Centroid;

use super::wkb_util::{read_wkb, wkb_to_geo, as_binary_array};

const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StDistanceSphere {
    signature: Signature,
}

impl Default for StDistanceSphere {
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

impl ScalarUDFImpl for StDistanceSphere {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_distancesphere" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Float64) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col1 = as_binary_array(&arrays[0])?;
        let col2 = as_binary_array(&arrays[1])?;

        let result: Float64Array = col1.iter().zip(col2.iter())
            .map(|(b1, b2)| {
                let g1 = wkb_to_geo(read_wkb(b1?).ok()?).ok()?;
                let g2 = wkb_to_geo(read_wkb(b2?).ok()?).ok()?;
                let c1 = g1.centroid()?;
                let c2 = g2.centroid()?;
                Some(haversine(c1.x(), c1.y(), c2.x(), c2.y()))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn haversine(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let to_rad = PI / 180.0;
    let dlat = (lat2 - lat1) * to_rad;
    let dlon = (lon2 - lon1) * to_rad;
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_METERS * a.sqrt().asin()
}
