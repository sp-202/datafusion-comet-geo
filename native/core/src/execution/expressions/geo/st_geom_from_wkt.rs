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
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geo_types::Geometry;
use wkt::Wkt;

use super::wkb_util::geom_to_wkb;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StGeomFromWkt {
    signature: Signature,
}

impl Default for StGeomFromWkt {
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

impl ScalarUDFImpl for StGeomFromWkt {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_geomfromwkt" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arr = &arrays[0];
        let col = arrow::compute::cast(arr, &DataType::Utf8)?;
        let col = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        let mut builder = BinaryBuilder::with_capacity(col.len(), col.len() * 64);
        for v in col.iter() {
            match v {
                Some(wkt_str) => {
                    let result = (|| -> Option<Vec<u8>> {
                        let wkt = Wkt::<f64>::from_str(wkt_str).ok()?;
                        let geom = Geometry::<f64>::try_from(wkt).ok()?;
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
