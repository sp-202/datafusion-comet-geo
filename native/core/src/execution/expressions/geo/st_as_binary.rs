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

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::wkb_util::as_binary_array;

/// ST_AsBinary / ST_AsWKB — return the WKB bytes of a geometry column.
///
/// Comet stores geometries as WKB internally, so this is a zero-copy passthrough
/// that makes the raw bytes available for writing to GeoParquet or external tools.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StAsBinary {
    signature: Signature,
}

impl Default for StAsBinary {
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

impl ScalarUDFImpl for StAsBinary {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_asbinary" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        // Normalise to Binary (handles LargeBinary / BinaryView transparently).
        let col = as_binary_array(&arrays[0])?;
        Ok(ColumnarValue::Array(Arc::new(col) as ArrayRef))
    }
}
