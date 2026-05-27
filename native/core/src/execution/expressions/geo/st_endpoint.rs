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

use arrow::array::{ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geo_types::{Geometry, Point};

use super::wkb_util::{as_binary_array, geom_to_wkb, read_wkb, wkb_to_geo};

fn unary_point_sig() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Binary]),
            TypeSignature::Exact(vec![DataType::LargeBinary]),
            TypeSignature::Exact(vec![DataType::BinaryView]),
        ],
        Volatility::Immutable,
    )
}

fn invoke_point<F>(
    args: ScalarFunctionArgs,
    extract: F,
) -> DataFusionResult<ColumnarValue>
where
    F: Fn(&geo_types::LineString) -> Option<Point>,
{
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let col = as_binary_array(&arrays[0])?;

    let mut builder = BinaryBuilder::with_capacity(col.len(), col.len() * 21);
    for bytes in col.iter() {
        match bytes {
            Some(b) => {
                let result = (|| -> Option<Vec<u8>> {
                    let g = wkb_to_geo(read_wkb(b).ok()?).ok()?;
                    let ls = match g {
                        Geometry::LineString(ls) => ls,
                        _ => return None,
                    };
                    let pt = extract(&ls)?;
                    geom_to_wkb(&Geometry::Point(pt)).ok()
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

/// ST_StartPoint — returns the first point of a LineString.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StStartPoint {
    signature: Signature,
}

impl Default for StStartPoint {
    fn default() -> Self {
        Self { signature: unary_point_sig() }
    }
}

impl ScalarUDFImpl for StStartPoint {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_startpoint" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke_point(args, |ls| ls.0.first().map(|c| Point::new(c.x, c.y)))
    }
}

/// ST_EndPoint — returns the last point of a LineString.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StEndPoint {
    signature: Signature,
}

impl Default for StEndPoint {
    fn default() -> Self {
        Self { signature: unary_point_sig() }
    }
}

impl ScalarUDFImpl for StEndPoint {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_endpoint" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke_point(args, |ls| ls.0.last().map(|c| Point::new(c.x, c.y)))
    }
}
