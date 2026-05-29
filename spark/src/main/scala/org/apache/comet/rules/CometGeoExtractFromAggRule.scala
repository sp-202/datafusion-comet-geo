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
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

import org.apache.comet.expressions.CometGeoExpression

/**
 * Physical plan rule and helper for extracting geo expressions out of HashAggregateExec
 * resultExpressions.
 *
 * When a query like `SELECT category, st_point(avg(lon), avg(lat)) FROM pts GROUP BY category`
 * reaches physical planning, Spark puts `st_point(avg_lon_attr, avg_lat_attr)` in the Final
 * HashAggregateExec's resultExpressions. CometHashAggregateExec.doConvert tries to serialize
 * these via exprToProto bound against aggregateAttributes (intermediate buffer cols) -- but
 * avg_lon_attr is a final output attribute, not a buffer attribute, so serialization fails and
 * the entire aggregate falls back to JVM.
 *
 * Fix: strip geo expressions from resultExpressions before serde, inject a ProjectExec above the
 * aggregate that re-applies the geo expressions to the plain output attributes.
 */
case class CometGeoExtractFromAggRule(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case agg: HashAggregateExec if CometGeoExtractFromAggRule.hasGeoInResults(agg) =>
      val (stripped, wrap) = CometGeoExtractFromAggRule.stripGeoFromResults(agg)
      wrap(stripped)
  }
}

object CometGeoExtractFromAggRule {

  def hasGeoInResults(agg: HashAggregateExec): Boolean =
    agg.resultExpressions.exists(e => containsGeo(unwrapAlias(e)))

  /**
   * Split resultExpressions into plain and geo-carrying. Returns:
   *   - a copy of the aggregate with only plain resultExpressions
   *   - a function that wraps any SparkPlan in a ProjectExec re-applying the geo exprs
   *
   * The geo exprs in resultExpressions already reference the agg's final output Attributes (Spark
   * resolved avg(lon) -> avg_lon_attr during physical planning), so they are safe to use directly
   * in the outer ProjectExec.
   */
  def stripGeoFromResults(agg: HashAggregateExec): (HashAggregateExec, SparkPlan => SparkPlan) = {
    val (geoExprs, plainExprs) =
      agg.resultExpressions.partition(e => containsGeo(unwrapAlias(e)))

    val strippedAgg = agg.copy(resultExpressions = plainExprs)

    val wrap: SparkPlan => SparkPlan = { child =>
      val plainAttrs: Seq[NamedExpression] = plainExprs.map(_.toAttribute)
      ProjectExec(plainAttrs ++ geoExprs, child)
    }

    (strippedAgg, wrap)
  }

  private def unwrapAlias(e: NamedExpression): Expression = e match {
    case Alias(child, _) => child
    case other => other
  }

  private def containsGeo(expr: Expression): Boolean =
    expr.isInstanceOf[CometGeoExpression] || expr.children.exists(containsGeo)
}
