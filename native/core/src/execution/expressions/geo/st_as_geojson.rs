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

use arrow::array::{Array, ArrayRef, BinaryArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use geo_traits::{GeometryTrait, GeometryType, PointTrait, PolygonTrait};

use super::wkb_util::{read_wkb, wkb_to_geo, as_binary_array};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StAsGeoJson {
    signature: Signature,
}

impl Default for StAsGeoJson {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                    TypeSignature::Exact(vec![DataType::BinaryView]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StAsGeoJson {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_asgeojson" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Utf8) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = as_binary_array(&arrays[0])?;

        let mut builder = StringBuilder::with_capacity(col.len(), col.len() * 33);
        for b in col.iter() {
            match b {
                Some(bytes) => {
                    let wkb = read_wkb(bytes)?;
                    let json_str = geom_to_geojson(&wkb)?;
                    builder.append_value(&json_str);
                }
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn geom_to_geojson(wkb: &wkb::reader::Wkb<'_>) -> DataFusionResult<String> {
    match wkb.as_type() {
        GeometryType::Point(pt) if pt.coord().is_none() => {
            return Ok(r#"{"type":"Point","coordinates":[]}"#.to_string());
        }
        GeometryType::Polygon(poly) if poly.exterior().is_none() => {
            return Ok(r#"{"type":"Polygon","coordinates":[]}"#.to_string());
        }
        _ => {}
    }

    let geo_geom = wkb_to_geo(wkb.clone())?;
    let geojson_value = geojson::GeometryValue::from(&geo_geom);
    let geojson_geom = geojson::Geometry::new(geojson_value);
    serde_json::to_string(&geojson_geom).map_err(|e| DataFusionError::External(Box::new(e)))
}
