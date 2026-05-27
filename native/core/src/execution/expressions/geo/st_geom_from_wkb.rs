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

use super::wkb_util::{as_binary_array, read_wkb, wkb_to_geo, geom_to_wkb};

/// ST_GeomFromWKB — parse a WKB binary column into a Comet geometry (also WKB).
///
/// This is a no-op in terms of bytes: it validates the WKB and re-emits it,
/// making WKB columns from GeoParquet files usable by all other ST_ functions.
/// Invalid WKB rows are returned as null rather than throwing.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StGeomFromWkb {
    signature: Signature,
}

impl Default for StGeomFromWkb {
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

impl ScalarUDFImpl for StGeomFromWkb {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_geomfromwkb" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = as_binary_array(&arrays[0])?;

        let mut builder = BinaryBuilder::with_capacity(col.len(), col.len() * 32);
        for bytes in col.iter() {
            match bytes {
                Some(b) => {
                    // Validate + round-trip through geo-types to normalise the WKB.
                    let result = (|| -> Option<Vec<u8>> {
                        let wkb = read_wkb(b).ok()?;
                        let geom = wkb_to_geo(wkb).ok()?;
                        geom_to_wkb(&geom).ok()
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
