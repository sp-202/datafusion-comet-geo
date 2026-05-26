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

use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};

use super::wkb_util::as_binary_array;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StGeometryType {
    signature: Signature,
}

impl Default for StGeometryType {
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

impl ScalarUDFImpl for StGeometryType {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_geometrytype" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Utf8) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = as_binary_array(&arrays[0])?;

        let mut builder = StringBuilder::with_capacity(col.len(), col.len() * 12);
        for b in col.iter() {
            match b {
                Some(bytes) => {
                    let name = infer_type_name(bytes)?;
                    builder.append_value(name);
                }
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn infer_type_name(buf: &[u8]) -> DataFusionResult<&'static str> {
    if buf.len() < 5 {
        return Err(DataFusionError::Execution(format!("Invalid WKB: {} bytes", buf.len())));
    }
    let code = match buf[0] {
        0 => u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
        1 => u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]),
        _ => return Err(DataFusionError::Execution("Invalid WKB byte order".into())),
    };
    match code & 0xFFFF {
        1 | 1001 | 2001 | 3001 => Ok("ST_Point"),
        2 | 1002 | 2002 | 3002 => Ok("ST_LineString"),
        3 | 1003 | 2003 | 3003 => Ok("ST_Polygon"),
        4 | 1004 | 2004 | 3004 => Ok("ST_MultiPoint"),
        5 | 1005 | 2005 | 3005 => Ok("ST_MultiLineString"),
        6 | 1006 | 2006 | 3006 => Ok("ST_MultiPolygon"),
        7 | 1007 | 2007 | 3007 => Ok("ST_GeometryCollection"),
        other => Err(DataFusionError::Execution(format!("Unknown WKB type: {other}"))),
    }
}
