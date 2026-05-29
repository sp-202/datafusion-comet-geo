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
    case agg: Aggregate if hasGeoInAggregateResults(agg) =>
      // Geo expr in the result projection (e.g. st_point(avg(lon), avg(lat))) - lift it
      // above the aggregate into a Project so it lands in CometProject, not JVM codegen.
      liftGeoFromAggregateResults(agg)
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

  // ---- Aggregate result projection -----------------------------------------

  /** True when a geo expression appears in the output of an Aggregate (not just its inputs). */
  private def hasGeoInAggregateResults(agg: Aggregate): Boolean =
    agg.aggregateExpressions.exists(e => containsGeo(unwrapAlias(e)))

  /**
   * Rewrite: Aggregate(keys, [avg(lon) AS avg_lon, st_point(avg(lon), avg(lat)) AS centroid])
   * Into: Project([avg_lon_attr, st_point(avg_lon_attr, avg_lat_attr) AS centroid]) +-
   * Aggregate(keys, [avg(lon) AS avg_lon, avg(lat) AS _geo_agg_lat])
   *
   * For each geo-carrying result expression, every AggregateExpression it references is ensured
   * to be emitted by the inner Aggregate (adding a new alias if not already present). The outer
   * Project replaces each AggregateExpression with the corresponding output Attribute, making the
   * geo expression safe to evaluate as a plain Project with no agg nodes inside it.
   */
  private def liftGeoFromAggregateResults(agg: Aggregate): LogicalPlan = {
    // Build a map from AggregateExpression canonical form -> existing output Attribute
    // for agg exprs that are already emitted by the Aggregate.
    val existingAggMap: Map[Expression, Attribute] = agg.aggregateExpressions
      .collect { case ne =>
        ne.toAttribute -> unwrapAlias(ne)
      }
      .collect { case (attr, ae: AggregateExpression) => ae -> attr }
      .toMap

    val extraAggExprs = scala.collection.mutable.ArrayBuffer.empty[NamedExpression]
    val aggExprToAttr = scala.collection.mutable.Map.empty[Expression, Attribute]
    aggExprToAttr ++= existingAggMap

    // Ensure every AggregateExpression reachable from expr is emitted by the Aggregate.
    // Returns the expression with AggregateExpression nodes replaced by their output Attributes.
    def replaceAggExprs(expr: Expression): Expression = expr match {
      case e if e.isInstanceOf[CometGeoExpression] =>
        e.mapChildren(replaceAggExprs)
      case ae: AggregateExpression =>
        aggExprToAttr.getOrElseUpdate(
          ae, {
            val a = Alias(ae, s"_geo_agg_${ae.hashCode().toHexString}")()
            extraAggExprs += a
            a.toAttribute
          })
      case other =>
        other.mapChildren(replaceAggExprs)
    }

    val (geoExprs, plainExprs) =
      agg.aggregateExpressions.partition(e => containsGeo(unwrapAlias(e)))

    // Build outer project expressions: geo-carrying exprs with AggExpr nodes replaced by attrs
    val outerGeoExprs: Seq[NamedExpression] = geoExprs.map {
      case Alias(child, name) => Alias(replaceAggExprs(child), name)()
      case other => replaceAggExprs(other).asInstanceOf[NamedExpression]
    }

    // New aggregate: plain non-geo exprs + any extra aliases needed for geo inputs
    val newAgg = agg.copy(aggregateExpressions = plainExprs ++ extraAggExprs.toSeq)

    // Outer project: plain agg outputs by attribute reference + geo exprs
    val outerList: Seq[NamedExpression] =
      plainExprs.map(_.toAttribute) ++ outerGeoExprs
    Project(outerList, newAgg)
  }

  private def unwrapAlias(e: NamedExpression): Expression = e match {
    case Alias(child, _) => child
    case other => other
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
