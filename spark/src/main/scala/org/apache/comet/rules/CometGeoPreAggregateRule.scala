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
    val allExprs = agg.groupingExpressions ++ agg.aggregateExpressions.flatMap(_.children)
    allExprs.exists(containsGeo)
  }

  private def rewriteAggregate(agg: Aggregate): Aggregate = {
    val (newChild, subst) = liftGeoExprs(
      agg.child,
      agg.groupingExpressions ++ agg.aggregateExpressions.flatMap(_.children))

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
   * Rewrite: Aggregate(..., [avg(lon) AS avg_lon, st_point(avg(lon), avg(lat)) AS centroid])
   * Into: Project([avg_lon, st_point(avg_lon_attr, avg_lat_attr) AS centroid]) +- Aggregate(...,
   * [avg(lon) AS avg_lon, avg(lat) AS avg_lat])
   *
   * Each aggregate result expression that contains a geo call is split: the underlying non-geo
   * aggregate computations stay inside the Aggregate, and the geo wrapping is pushed into an
   * outer Project that CometExecRule can convert to CometProject.
   */
  private def liftGeoFromAggregateResults(agg: Aggregate): LogicalPlan = {
    // Separate aggregate exprs into geo-carrying and plain
    val (geoExprs, plainExprs) =
      agg.aggregateExpressions.partition(e => containsGeo(unwrapAlias(e)))

    // For each geo-carrying expr, replace the geo parts with fresh attribute refs
    // and collect the inner non-geo exprs that the agg must emit
    val extraAggExprs = scala.collection.mutable.ArrayBuffer.empty[NamedExpression]
    val subst = scala.collection.mutable.Map.empty[Expression, Attribute]

    def extractAggLeaves(expr: Expression): Expression = expr match {
      case e if e.isInstanceOf[CometGeoExpression] =>
        // Children of the geo expr are either plain agg results or further geo - recurse
        e.mapChildren(extractAggLeaves)
      case e if !containsGeo(e) && e.children.nonEmpty =>
        // Non-geo sub-expression with children: this is an agg function or derived expr
        // that the Aggregate must compute - alias it and replace with its attribute
        subst.getOrElseUpdate(
          e, {
            val a = Alias(e, s"_geo_agg_${e.hashCode().toHexString}")()
            extraAggExprs += a
            a.toAttribute
          })
      case other => other
    }

    val outerProjectExprs: Seq[NamedExpression] = geoExprs.map {
      case Alias(child, name) =>
        val newChild = extractAggLeaves(child)
        Alias(newChild, name)()
      case other =>
        val newExpr = extractAggLeaves(other)
        newExpr.asInstanceOf[NamedExpression]
    }

    // New aggregate emits plain exprs + extra aliases for geo inputs
    val newAgg = agg.copy(aggregateExpressions = plainExprs ++ extraAggExprs.toSeq)

    // Outer project: all plain agg outputs (by attribute) + geo result exprs
    val plainAttrs: Seq[NamedExpression] = plainExprs.map(_.toAttribute)
    val outerList: Seq[NamedExpression] = plainAttrs ++ outerProjectExprs
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
