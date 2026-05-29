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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

import org.apache.comet.expressions.CometGeoExpression

/**
 * Physical plan rule helper for extracting geo expressions out of HashAggregateExec
 * resultExpressions.
 *
 * When a query like `SELECT category, st_point(avg(lon), avg(lat)) FROM pts GROUP BY category`
 * reaches physical planning, Spark puts `st_point(avg_lon_attr, avg_lat_attr)` in the Final
 * HashAggregateExec's resultExpressions. CometHashAggregateExec.doConvert tries to serialize
 * these via exprToProto bound against aggregateAttributes (intermediate buffer cols) -- but
 * avg_lon_attr is a final output attribute, not a buffer attribute, so serialization fails and
 * the entire aggregate falls back to JVM.
 *
 * Fix (in CometExecRule.convertToComet): strip geo expressions from resultExpressions before
 * serde, convert the stripped aggregate natively, then build a CometProjectExec above it that
 * re-applies the geo exprs bound against the stripped agg's output attributes.
 */
case class CometGeoExtractFromAggRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan
}

object CometGeoExtractFromAggRule {

  def hasGeoInResults(agg: HashAggregateExec): Boolean =
    agg.resultExpressions.exists(e => isTopLevelGeo(unwrapAlias(e)))

  /**
   * Returns true only when the expression itself is a geo expression (not wrapped in non-geo
   * functions like toprettystring). This prevents show() display plans from triggering geo
   * extraction -- in those plans Spark wraps every result in toprettystring(...).
   */
  def isTopLevelGeo(expr: Expression): Boolean = expr.isInstanceOf[CometGeoExpression]

  /**
   * Split resultExpressions into plain and geo-carrying. Returns:
   *   - a copy of the aggregate with only plain resultExpressions
   *   - the full project list (plain attrs ++ rewritten geo exprs) for the CometProjectExec
   *
   * The geo exprs reference intermediate agg result attributes (e.g. avg(lon)#378) which are
   * inside Alias wrappers in plainExprs (e.g. Alias(avg(lon)#378, "avg_lon")#374). We must
   * substitute these inner references with the alias output attributes (#374) so the geo exprs
   * bind correctly against stripped.output when building the outer CometProjectExec.
   */
  def stripGeoFromResults(agg: HashAggregateExec): (HashAggregateExec, Seq[NamedExpression]) = {
    val (geoExprs, plainExprs) =
      agg.resultExpressions.partition(e => isTopLevelGeo(unwrapAlias(e)))

    val strippedAgg = agg.copy(resultExpressions = plainExprs)

    // Build substitution: alias.child (e.g. avg(lon)#378) -> alias.toAttribute (avg_lon#374)
    val subst: Map[Expression, Attribute] = plainExprs.collect { case a: Alias =>
      a.child -> a.toAttribute
    }.toMap

    val rewrittenGeoExprs: Seq[NamedExpression] = geoExprs.map { e =>
      substituteRefs(e, subst).asInstanceOf[NamedExpression]
    }

    val geoProjectList: Seq[NamedExpression] = plainExprs.map(_.toAttribute) ++ rewrittenGeoExprs

    (strippedAgg, geoProjectList)
  }

  private def substituteRefs(expr: Expression, subst: Map[Expression, Attribute]): Expression =
    subst.get(expr) match {
      case Some(attr) => attr
      case None => expr.mapChildren(substituteRefs(_, subst))
    }

  def unwrapAlias(e: NamedExpression): Expression = e match {
    case Alias(child, _) => child
    case other => other
  }

  def containsGeoExpr(expr: Expression): Boolean =
    expr.isInstanceOf[CometGeoExpression] || expr.children.exists(containsGeoExpr)
}
