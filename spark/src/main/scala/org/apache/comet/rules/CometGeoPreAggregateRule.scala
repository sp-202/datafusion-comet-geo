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
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, Window}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.comet.expressions.CometGeoExpression

/**
 * Rewrites Aggregate and Window nodes that reference geo expressions in their grouping keys,
 * aggregate inputs, or window specs into a two-level plan:
 *
 * Project (pre-materialize geo exprs as typed scalar aliases) +-- original child
 *
 * followed by the original Aggregate/Window operating only on the scalar Attribute references.
 * This ensures geo UDFs are always evaluated inside CometProject (above CometNativeScan) and
 * never re-evaluated by JVM HashAggregateExec or WindowExec codegen, which has no native geo
 * path.
 */
case class CometGeoPreAggregateRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case agg: Aggregate if hasGeoInAggregateExprs(agg) =>
      rewriteAggregate(agg)
    case win: Window if hasGeoInWindowExprs(win) =>
      rewriteWindow(win)
  }

  // ---- Aggregate -----------------------------------------------------------

  private def hasGeoInAggregateExprs(agg: Aggregate): Boolean = {
    // Only match geo that is INSIDE an AggregateExpression's arguments (e.g. avg(st_area(...))).
    // Geo that wraps agg outputs (e.g. st_point(avg(lon), avg(lat))) is handled by
    // hasGeoInAggregateResults - matching it here causes a pre-project below the agg
    // which tries to evaluate st_point before avg is computed.
    val aggFuncArgs = agg.aggregateExpressions.flatMap(collectAggFuncArgs)
    val inputExprs = agg.groupingExpressions ++ aggFuncArgs
    inputExprs.exists(containsGeo)
  }

  /** Collect arguments of AggregateExpression nodes - the expressions fed INTO agg functions. */
  private def collectAggFuncArgs(expr: Expression): Seq[Expression] = expr match {
    case ae: AggregateExpression => ae.aggregateFunction.children
    case other => other.children.flatMap(collectAggFuncArgs)
  }

  private def rewriteAggregate(agg: Aggregate): Aggregate = {
    val aggFuncArgs = agg.aggregateExpressions.flatMap(collectAggFuncArgs)
    val (newChild, subst) = liftGeoExprs(agg.child, agg.groupingExpressions ++ aggFuncArgs)

    val newGrouping = agg.groupingExpressions.map(replaceGeo(_, subst))
    val newAggExprs =
      agg.aggregateExpressions.map(e => replaceGeo(e, subst).asInstanceOf[NamedExpression])

    agg.copy(
      groupingExpressions = newGrouping,
      aggregateExpressions = newAggExprs,
      child = newChild)
  }

  // ---- Window --------------------------------------------------------------

  private def hasGeoInWindowExprs(win: Window): Boolean = {
    val allExprs = win.windowExpressions.flatMap(_.children) ++
      win.partitionSpec ++ win.orderSpec.map(_.child)
    allExprs.exists(containsGeo)
  }

  private def rewriteWindow(win: Window): Window = {
    val candidateExprs = win.windowExpressions.flatMap(_.children) ++
      win.partitionSpec ++ win.orderSpec.map(_.child)

    val (newChild, subst) = liftGeoExprs(win.child, candidateExprs)

    val newWindowExprs =
      win.windowExpressions.map(e => replaceGeo(e, subst).asInstanceOf[NamedExpression])
    val newPartitionSpec = win.partitionSpec.map(replaceGeo(_, subst))
    val newOrderSpec = win.orderSpec.map(s => s.copy(child = replaceGeo(s.child, subst)))

    win.copy(
      windowExpressions = newWindowExprs,
      partitionSpec = newPartitionSpec,
      orderSpec = newOrderSpec,
      child = newChild)
  }

  // ---- Shared helpers ------------------------------------------------------

  /** Collect all geo sub-expressions, alias each unique one, wrap child in Project. */
  private def liftGeoExprs(
      child: LogicalPlan,
      exprs: Seq[Expression]): (LogicalPlan, Map[Expression, Attribute]) = {

    val geoExprs = exprs.flatMap(collectGeo).distinct

    if (geoExprs.isEmpty) return (child, Map.empty)

    // Build aliases for each unique geo expression
    val aliases: Seq[Alias] = geoExprs.map { expr =>
      Alias(expr, s"_geo_pre_${expr.hashCode().toHexString}")()
    }

    // The new Project outputs all existing child output cols + the new aliases
    val newProjectList: Seq[NamedExpression] =
      child.output.map(a => a: NamedExpression) ++ aliases

    val newChild = Project(newProjectList, child)

    // Substitution map: original geo expr -> the alias's output Attribute
    val subst: Map[Expression, Attribute] =
      aliases.map(a => a.child -> a.toAttribute).toMap

    (newChild, subst)
  }

  /** Replace every geo sub-expression in `expr` using the substitution map. */
  private def replaceGeo(expr: Expression, subst: Map[Expression, Attribute]): Expression = {
    subst.get(expr) match {
      case Some(attr) => attr
      case None => expr.mapChildren(replaceGeo(_, subst))
    }
  }

  private def containsGeo(expr: Expression): Boolean =
    expr.isInstanceOf[CometGeoExpression] || expr.children.exists(containsGeo)

  private def collectGeo(expr: Expression): Seq[Expression] = {
    if (expr.isInstanceOf[CometGeoExpression]) Seq(expr)
    else expr.children.flatMap(collectGeo)
  }
}
