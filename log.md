<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Comet Columnar Transitions & Fallback Implementation Log

We have overhauled how Comet handles boundaries between native (`CometNativeExec`) and JVM (`SparkPlan`) operators, allowing for safe native re-entry after fallback without the pitfalls of manual wrapping.

## 1. Spark's Native Columnar API Usage
Instead of manually injecting `CometSparkToColumnarExec` at arbitrary points (which causes crashes and edge cases), we now rely on Spark's own `ApplyColumnarRulesAndInsertTransitions` pass. Spark automatically detects boundaries using the `supportsColumnar` property and inserts `RowToColumnarExec` or `ColumnarToRowExec` as needed.

## 2. Introducing `CometColumnarRule` (with Targeted Scoping)
We created `CometColumnarRule`, a post-processing rule registered in `CometExecColumnar.postColumnarTransitions`.
This rule safely replaces Spark's automatically inserted `RowToColumnarExec` with Comet's high-performance `CometSparkToColumnarExec`.

To prevent **Edge Case 10 (Schema Mismatch)** and the **"nested query"** issues where a non-Comet JVM query wraps a Comet query or vice-versa, we implemented targeted scoping:
- The rule uses `transformDown` and **only** replaces `RowToColumnarExec` when it is a direct child of a `CometPlan`. This ensures we don't accidentally inject Arrow batches into standard Spark columnar operators or subqueries that expect Spark's internal columnar format.
- We deliberately omitted `ColumnarToRowExec` replacement from this rule because `EliminateRedundantTransitions` already handles replacing it safely with `CometColumnarToRowExec` (or native) ONLY when the child is a Comet plan.

By doing this *after* Spark's native pass, we ensure that:
- Structural integrity is maintained across stage boundaries (AQE isolation is respected)
- Subquery rewriting and logical link setup remain intact
- Nested subqueries containing Comet nodes properly transition back to rows when evaluated by Spark's `SubqueryExec`.

## 3. Modifying `CometExecRule` with Strict Safety Guards
Previously, `CometExecRule` gave up converting an operator entirely if any of its children were not native, preventing native re-entry. We removed this restriction so it can convert operators even if they have JVM children (which will now be seamlessly wrapped by Spark's transition framework).

However, to address critical edge cases, we added a strict `safeToConvert` guard in `CometExecRule`:
- **Schema Support**: Must be fully compatible with Arrow conversion (`CometSparkToColumnarExec.isSchemaSupported`). This automatically protects against unsupported outputs like `ArrayType` (e.g., from a JVM `collect_set` buffer).
- **Exclusions**: `BroadcastExchangeExec`, `BroadcastQueryStageExec`, `ShuffleQueryStageExec`, `ReusedExchangeExec`, `AQEShuffleReadExec`, `WriteFilesExec`, and `DataWritingCommandExec` are explicitly blocked from conversion.
- **Unsafe Partial Aggregates (The `collect_set` Issue)**: Checks for the `COMET_UNSAFE_PARTIAL` tag to prevent buffer mismatch crashes. If a Final aggregate (like `collect_set`) falls back to JVM, the corresponding Partial aggregate is tagged. Our rule detects this tag and forces the entire block to stay in JVM, ensuring the aggregation buffers match perfectly.
- **Reverted Columnar Shuffles**: Checks for the `SKIP_COMET_SHUFFLE_TAG` to prevent converting shuffles that AQE has explicitly reverted to Spark format.

## 4. How "Native Wrapping JVM" Works Safely
When a native Comet operator wraps a JVM operator (e.g., `CometNativeExec` -> `JVM Operator`), Spark automatically detects the boundary (`supportsColumnar=true` over `supportsColumnar=false`) and inserts `RowToColumnarExec`. 
Our scoped `CometColumnarRule` then safely replaces it with `CometSparkToColumnarExec`, converting the JVM's row output into an Arrow `ColumnarBatch` for the native operator. 
Because we strictly validate the JVM child's schema in `CometExecRule` *before* converting the parent, we are guaranteed that `CometSparkToColumnarExec` will never crash on an unsupported type (like `CalendarIntervalType` or internal aggregation buffers).

These changes bring Comet in line with Photon and Gluten's architectural approach while preventing documented edge cases related to manual `tryBridgeWithR2C` wrapping.
