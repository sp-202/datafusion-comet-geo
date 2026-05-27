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

use arrow::array::{ArrayRef, BinaryBuilder, Float64Array};
use arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use geo::Translate;

use super::wkb_util::{as_binary_array, geom_to_wkb, read_wkb, wkb_to_geo};

/// ST_Translate — shift a geometry by (delta_x, delta_y).
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StTranslate {
    signature: Signature,
}

impl Default for StTranslate {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Float64, DataType::Float64]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Float64, DataType::Float64]),
                    TypeSignature::Exact(vec![DataType::BinaryView, DataType::Float64, DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StTranslate {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "st_translate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> { Ok(DataType::Binary) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let col = as_binary_array(&arrays[0])?;
        let dx = arrays[1].as_any().downcast_ref::<Float64Array>().unwrap();
        let dy = arrays[2].as_any().downcast_ref::<Float64Array>().unwrap();

        let mut builder = BinaryBuilder::with_capacity(col.len(), col.len() * 64);
        for i in 0..col.len() {
            if col.is_null(i) || dx.is_null(i) || dy.is_null(i) {
                builder.append_null();
                continue;
            }
            let result = (|| -> Option<Vec<u8>> {
                let b = col.value(i);
                let mut g = wkb_to_geo(read_wkb(b).ok()?).ok()?;
                g.translate_mut(dx.value(i), dy.value(i));
                geom_to_wkb(&g).ok()
            })();
            match result {
                Some(wkb) => builder.append_value(&wkb),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}
