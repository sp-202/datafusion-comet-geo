/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.CometSparkToColumnarExec
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}

/**
 * A post-columnar transition rule that replaces Spark's default [[RowToColumnarExec]]
 * (inserted at native/JVM boundaries by [[org.apache.spark.sql.execution.ApplyColumnarRulesAndInsertTransitions]])
 * with Comet's [[CometSparkToColumnarExec]], which converts JVM row/columnar output into
 * Arrow `ColumnarBatch` for downstream native operators.
 *
 * Replacement is intentionally scoped: only a [[RowToColumnarExec]] that is a **direct
 * child of a [[org.apache.spark.sql.comet.CometPlan]]** is replaced. This ensures we never
 * inject Arrow batches into a standard Spark columnar operator or into a nested JVM
 * subquery that expects Spark's internal columnar format.
 *
 * [[org.apache.spark.sql.execution.ColumnarToRowExec]] replacement is deliberately omitted
 * here because [[EliminateRedundantTransitions]] already handles that safely and selectively
 * (replacing with [[org.apache.spark.sql.comet.CometColumnarToRowExec]] or the native
 * [[org.apache.spark.sql.comet.CometNativeColumnarToRowExec]] only when the child is a
 * Comet plan).
 */
case class CometColumnarRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case p: org.apache.spark.sql.comet.CometPlan =>
      val newChildren = p.children.map {
        case r: RowToColumnarExec => CometSparkToColumnarExec(r.child)
        // We leave ColumnarToRowExec replacement to EliminateRedundantTransitions
        // which already handles it safely and selectively.
        case other => other
      }
      p.withNewChildren(newChildren)
  }
}
