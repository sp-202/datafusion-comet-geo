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

use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geo_traits::{GeometryTrait, PointTrait, PolygonTrait};

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
                    // SedonaDB pattern (sedona-db/rust/sedona-geo/src/st_asgeojson.rs):
                    // per-row errors produce null, not a batch failure
                    match geom_to_geojson(bytes) {
                        Ok(json_str) => builder.append_value(&json_str),
                        Err(_) => builder.append_null(),
                    }
                }
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn geom_to_geojson(bytes: &[u8]) -> DataFusionResult<String> {
    let wkb = read_wkb(bytes)?;

    // SedonaDB special-case handling for empty geometries geo_types cannot represent
    match wkb.as_type() {
        geo_traits::GeometryType::Point(pt) if pt.coord().is_none() => {
            return Ok(r#"{"type":"Point","coordinates":[]}"#.to_string());
        }
        geo_traits::GeometryType::Polygon(poly) if poly.exterior().is_none() => {
            return Ok(r#"{"type":"Polygon","coordinates":[]}"#.to_string());
        }
        _ => {}
    }

    let geo_geom = wkb_to_geo(wkb)?;
    let geojson_value = geojson::GeometryValue::from(&geo_geom);
    let geojson_geom = geojson::Geometry::new(geojson_value);
    serde_json::to_string(&geojson_geom).map_err(|e| DataFusionError::External(Box::new(e)))
}
