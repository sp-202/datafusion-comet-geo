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
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use geo::relate::Relate;

use super::wkb_util::{read_wkb, wkb_to_geo, as_binary_array};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StWithin {
    signature: Signature,
}

impl Default for StWithin {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::LargeBinary]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::LargeBinary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Binary]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StWithin {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_within" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Boolean) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col1 = as_binary_array(&arrays[0])?;
        let col2 = as_binary_array(&arrays[1])?;

        let result: BooleanArray = col1.iter().zip(col2.iter())
            .map(|(b1, b2)| {
                let g1 = wkb_to_geo(read_wkb(b1?).ok()?).ok()?;
                let g2 = wkb_to_geo(read_wkb(b2?).ok()?).ok()?;
                Some(g1.relate(&g2).is_within())
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
