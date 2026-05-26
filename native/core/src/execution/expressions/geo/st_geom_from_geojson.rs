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
use datafusion::common::cast::as_string_view_array;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::wkb_util::geom_to_wkb;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StGeomFromGeoJson {
    signature: Signature,
}

impl Default for StGeomFromGeoJson {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StGeomFromGeoJson {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_geomfromgeojson" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arg_array = ColumnarValue::Array(arrays[0].clone())
            .cast_to(&DataType::Utf8View, None)?
            .to_array(arrays[0].len())?;
        let col = as_string_view_array(&arg_array)?;

        let mut builder = BinaryBuilder::with_capacity(col.len(), col.len() * 64);
        for v in col.iter() {
            match v {
                Some(json_str) => {
                    let result = (|| -> Option<Vec<u8>> {
                        let gj: geojson::Geometry = json_str.parse().ok()?;
                        let geom = geo::Geometry::<f64>::try_from(&gj).ok()?;
                        geom_to_wkb(&geom).ok()
                    })();
                    match result {
                        Some(bytes) => builder.append_value(&bytes),
                        None => builder.append_null(),
                    }
                }
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}
