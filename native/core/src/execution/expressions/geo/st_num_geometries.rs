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

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geo_types::Geometry;

use super::wkb_util::{as_binary_array, read_wkb, wkb_to_geo};

/// ST_NumGeometries — returns the number of geometries in a collection,
/// or 1 for single geometries.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StNumGeometries {
    signature: Signature,
}

impl Default for StNumGeometries {
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

impl ScalarUDFImpl for StNumGeometries {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_numgeometries" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Int32) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = as_binary_array(&arrays[0])?;

        let result: Int32Array = col
            .iter()
            .map(|b| {
                b.and_then(|bytes| {
                    let g = wkb_to_geo(read_wkb(bytes).ok()?).ok()?;
                    Some(num_geometries(&g))
                })
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn num_geometries(geom: &Geometry) -> i32 {
    match geom {
        Geometry::MultiPoint(mp) => mp.0.len() as i32,
        Geometry::MultiLineString(ml) => ml.0.len() as i32,
        Geometry::MultiPolygon(mp) => mp.0.len() as i32,
        Geometry::GeometryCollection(gc) => gc.0.len() as i32,
        _ => 1,
    }
}
