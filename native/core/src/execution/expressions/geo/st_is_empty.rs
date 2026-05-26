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

use arrow::array::{ArrayRef, BinaryArray, BooleanArray};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use geo_traits::{GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait};

use super::wkb_util::read_wkb;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StIsEmpty {
    signature: Signature,
}

impl Default for StIsEmpty {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StIsEmpty {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_isempty" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Boolean) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = arrays[0].as_any().downcast_ref::<BinaryArray>().unwrap();

        let result: BooleanArray = col.iter()
            .map(|b| {
                let wkb = read_wkb(b?).ok()?;
                Some(is_wkb_empty(&wkb))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn is_wkb_empty(wkb: &wkb::reader::Wkb<'_>) -> bool {
    match wkb.as_type() {
        GeometryType::Point(p) => p.coord().is_none(),
        GeometryType::LineString(ls) => ls.num_coords() == 0,
        GeometryType::Polygon(poly) => poly.exterior().is_none(),
        GeometryType::MultiPoint(mp) => mp.num_points() == 0,
        GeometryType::MultiLineString(mls) => mls.num_line_strings() == 0,
        GeometryType::MultiPolygon(mpoly) => mpoly.num_polygons() == 0,
        GeometryType::GeometryCollection(gc) => gc.num_geometries() == 0,
        _ => false,
    }
}
