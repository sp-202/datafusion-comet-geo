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
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}

/**
 * A post-columnar transition rule that replaces Spark's default [[RowToColumnarExec]] and
 * [[ColumnarToRowExec]] with Comet's native [[CometSparkToColumnarExec]] and
 * [[CometColumnarToRowExec]].
 *
 * This allows Comet to leverage Spark's native [[org.apache.spark.sql.execution.ApplyColumnarRulesAndInsertTransitions]]
 * mechanism to automatically determine when boundary transitions are required between native
 * columnar formats and Spark JVM row formats, enabling seamless partial rollouts and native
 * re-entry (similar to Gluten and Photon engines).
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
