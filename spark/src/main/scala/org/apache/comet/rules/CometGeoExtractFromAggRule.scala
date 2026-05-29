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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

import org.apache.comet.expressions.CometGeoExpression

/**
 * Physical plan rule that runs after CometExecRule. Detects a Final-mode HashAggregateExec whose
 * resultExpressions contain geo expressions (e.g. st_point(avg(lon), avg(lat))). Inserts a
 * ProjectExec above it that evaluates the geo expressions, leaving the aggregate to only emit the
 * plain numeric results.
 *
 * This is a physical-level rewrite so it is immune to Spark's logical optimizer re-matching
 * cycles that caused the max-iterations warning when this was done at the logical plan level.
 *
 * After this rule fires, CometExecRule (which has already run) will NOT re-convert the new
 * ProjectExec. Instead CometColumnarRule and the re-entry bridge (CometSparkToColumnarExec)
 * handle it in postColumnarTransitions so the geo Project becomes a CometProject executing
 * natively.
 *
 * NOTE: this rule runs as a preColumnarTransitions rule, which means it runs before columnar
 * transitions are inserted. The newly injected ProjectExec will be seen by CometExecRule on the
 * next AQE re-optimization pass and converted to CometProject at that point.
 */
case class CometGeoExtractFromAggRule(session: SparkSession)
    extends Rule[SparkPlan]
    with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach {
      case agg: HashAggregateExec =>
        logInfo(
          s"[GeoExtract] HashAggregateExec modes=${agg.aggregateExpressions.map(_.mode).distinct}" +
            s" resultExprs=${agg.resultExpressions.map(e => e.getClass.getSimpleName + ":" + e)}")
      case _ =>
    }
    plan.transformUp {
      case agg: HashAggregateExec if hasGeoInResults(agg) =>
        extractGeoFromResults(agg)
    }
  }

  private def hasGeoInResults(agg: HashAggregateExec): Boolean =
    agg.resultExpressions.exists(e => containsGeo(unwrapAlias(e)))

  /**
   * Split the aggregate's resultExpressions into plain (no geo) and geo-carrying. The aggregate
   * emits the plain results plus any extra attributes needed as geo inputs. A new ProjectExec
   * above computes the geo expressions from those plain attributes.
   *
   * Before: HashAggregateExec resultExpressions=[cnt, avg_lat, avg_lon, st_point(avg_lon_attr,
   * avg_lat_attr) AS centroid]
   *
   * After: ProjectExec [cnt, avg_lat, avg_lon, st_point(avg_lon_attr, avg_lat_attr) AS centroid]
   * \+- HashAggregateExec resultExpressions=[cnt, avg_lat, avg_lon]
   */
  private def extractGeoFromResults(agg: HashAggregateExec): SparkPlan = {
    val (geoExprs, plainExprs) =
      agg.resultExpressions.partition(e => containsGeo(unwrapAlias(e)))

    val plainAttrs: Seq[NamedExpression] = plainExprs.map(_.toAttribute)

    // The geo exprs already reference the agg's output attributes directly
    // (Spark has already resolved avg(lon) -> avg_lon_attr in resultExpressions)
    val outerProjectList: Seq[NamedExpression] = plainAttrs ++ geoExprs

    val newAgg = agg.copy(resultExpressions = plainExprs)
    ProjectExec(outerProjectList, newAgg)
  }

  private def unwrapAlias(e: NamedExpression): Expression = e match {
    case Alias(child, _) => child
    case other => other
  }

  private def containsGeo(expr: Expression): Boolean =
    expr.isInstanceOf[CometGeoExpression] || expr.children.exists(containsGeo)
}
