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
use geo::SimplifyVwPreserve;
use geo_types::Geometry;

use super::wkb_util::{geom_to_wkb, read_wkb, wkb_to_geo, as_binary_array};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StSimplifyPreserveTopology {
    signature: Signature,
}

impl Default for StSimplifyPreserveTopology {
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

impl ScalarUDFImpl for StSimplifyPreserveTopology {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_simplifypreservetopology" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        // SedonaDB pattern: cast tolerance to Float64 scalar before the loop
        let tol_casted = args.args[1].cast_to(&DataType::Float64, None)?;
        let tolerance: Option<f64> = if let ColumnarValue::Scalar(ref sv) = tol_casted {
            if sv.is_null() { None } else { Some(f64::try_from(sv.clone()).map_err(|e| DataFusionError::External(Box::new(e)))?) }
        } else {
            return Err(DataFusionError::Execution("st_simplifypreservetopology: tolerance must be a scalar".into()));
        };

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geom_col = as_binary_array(&arrays[0])?;

        let mut builder = BinaryBuilder::with_capacity(geom_col.len(), geom_col.len() * 64);
        for b in geom_col.iter() {
            match (b, tolerance) {
                (Some(bytes), Some(tol)) => {
                    let result = (|| -> Option<Vec<u8>> {
                        let g = wkb_to_geo(read_wkb(bytes).ok()?).ok()?;
                        let simplified = simplify_vw_preserve_geom(&g, tol);
                        geom_to_wkb(&simplified).ok()
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

fn simplify_vw_preserve_geom(geom: &Geometry, epsilon: f64) -> Geometry {
    match geom {
        Geometry::LineString(ls) => Geometry::LineString(ls.simplify_vw_preserve(&epsilon)),
        Geometry::MultiLineString(mls) => Geometry::MultiLineString(mls.simplify_vw_preserve(&epsilon)),
        Geometry::Polygon(p) => Geometry::Polygon(p.simplify_vw_preserve(&epsilon)),
        Geometry::MultiPolygon(mp) => Geometry::MultiPolygon(mp.simplify_vw_preserve(&epsilon)),
        Geometry::GeometryCollection(gc) => Geometry::GeometryCollection(
            geo_types::GeometryCollection(gc.0.iter().map(|g| simplify_vw_preserve_geom(g, epsilon)).collect())
        ),
        _ => geom.clone(),
    }
}
