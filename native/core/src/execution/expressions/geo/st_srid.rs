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

use super::wkb_util::as_binary_array;

/// ST_SRID — returns the SRID embedded in EWKB, or 0 for standard WKB.
///
/// Standard OGC WKB does not carry an SRID. PostGIS EWKB embeds it after the
/// geometry type word when the SRID flag (0x20000000) is set. This function
/// returns the embedded SRID when present, 0 otherwise. Comet writes standard
/// WKB (no SRID), so this is primarily useful when reading external EWKB data.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StSrid {
    signature: Signature,
}

impl Default for StSrid {
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

impl ScalarUDFImpl for StSrid {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_srid" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Int32) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = as_binary_array(&arrays[0])?;

        let result: Int32Array = col
            .iter()
            .map(|b| b.and_then(|bytes| extract_srid(bytes)))
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

/// Extract SRID from EWKB bytes if the SRID flag is set, otherwise return 0.
fn extract_srid(bytes: &[u8]) -> Option<i32> {
    if bytes.len() < 5 {
        return None;
    }
    let little_endian = bytes[0] == 1;
    let type_word = if little_endian {
        u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
    } else {
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
    };

    let has_srid = (type_word & 0x20000000) != 0;
    if has_srid && bytes.len() >= 9 {
        let srid = if little_endian {
            i32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]])
        } else {
            i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]])
        };
        Some(srid)
    } else {
        Some(0)
    }
}
