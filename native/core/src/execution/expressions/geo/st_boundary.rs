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

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use geo_types::{Geometry, LineString, MultiLineString, MultiPoint};

use super::wkb_util::{geom_to_wkb, read_wkb, wkb_to_geo, as_binary_array};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StBoundary {
    signature: Signature,
}

impl Default for StBoundary {
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

impl ScalarUDFImpl for StBoundary {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_boundary" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = as_binary_array(&arrays[0])?;

        let mut builder = BinaryBuilder::with_capacity(col.len(), col.len() * 64);
        for b in col.iter() {
            match b {
                Some(bytes) => {
                    let result = (|| -> Option<Vec<u8>> {
                        let g = wkb_to_geo(read_wkb(bytes).ok()?).ok()?;
                        let b = boundary(&g)?;
                        geom_to_wkb(&b).ok()
                    })();
                    match result {
                        Some(wkb) => builder.append_value(&wkb),
                        None => builder.append_null(),
                    }
                }
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn boundary(geom: &Geometry) -> Option<Geometry> {
    match geom {
        Geometry::Point(_) => Some(Geometry::GeometryCollection(geo_types::GeometryCollection(vec![]))),
        Geometry::LineString(ls) => {
            let pts = vec![
                geo_types::Point(ls.0.first()?.clone()),
                geo_types::Point(ls.0.last()?.clone()),
            ];
            Some(Geometry::MultiPoint(MultiPoint(pts)))
        }
        Geometry::Polygon(p) => {
            let mut rings: Vec<LineString<f64>> = vec![p.exterior().clone()];
            rings.extend(p.interiors().iter().cloned());
            Some(Geometry::MultiLineString(MultiLineString(rings)))
        }
        Geometry::MultiLineString(mls) => {
            let pts: Vec<geo_types::Point<f64>> = mls.0.iter().flat_map(|ls| {
                ls.0.first().map(|c| geo_types::Point(c.clone())).into_iter()
                    .chain(ls.0.last().map(|c| geo_types::Point(c.clone())).into_iter())
            }).collect();
            Some(Geometry::MultiPoint(MultiPoint(pts)))
        }
        Geometry::MultiPolygon(mp) => {
            let rings: Vec<LineString<f64>> = mp.0.iter().flat_map(|p| {
                std::iter::once(p.exterior().clone())
                    .chain(p.interiors().iter().cloned())
            }).collect();
            Some(Geometry::MultiLineString(MultiLineString(rings)))
        }
        _ => None,
    }
}
