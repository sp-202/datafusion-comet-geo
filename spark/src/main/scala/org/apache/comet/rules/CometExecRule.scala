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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Divide, DoubleLiteral, EqualNullSafe, EqualTo, Expression, FloatLiteral, GreaterThan, GreaterThanOrEqual, KnownFloatingPointNormalized, LessThan, LessThanOrEqual, Literal, NamedExpression, Remainder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, Final, Partial}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2CommandExec}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, CometExplainInfo, ExtendedExplainInfo}
import org.apache.comet.CometConf.{COMET_SPARK_TO_ARROW_ENABLED, COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST}
import org.apache.comet.CometSparkSessionExtensions._
import org.apache.comet.expressions.CometGeoExpression
import org.apache.comet.rules.CometExecRule.allExecs
import org.apache.comet.serde._
import org.apache.comet.serde.operator._
import org.apache.comet.shims.{ShimCometStreaming, ShimSubqueryBroadcast}

object CometExecRule {

  /**
   * Tag applied to Partial-mode aggregate operators that must NOT be converted to Comet because
   * the corresponding Final-mode aggregate cannot be converted, and the aggregate functions have
   * incompatible intermediate buffer formats between Spark and Comet.
   */
  val COMET_UNSAFE_PARTIAL: TreeNodeTag[String] =
    TreeNodeTag[String]("comet.unsafePartialAgg")

  /**
   * Fully native operators.
   */
  val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(
      classOf[ProjectExec] -> CometProjectExec,
      classOf[FilterExec] -> CometFilterExec,
      classOf[LocalLimitExec] -> CometLocalLimitExec,
      classOf[GlobalLimitExec] -> CometGlobalLimitExec,
      classOf[ExpandExec] -> CometExpandExec,
      classOf[GenerateExec] -> CometExplodeExec,
      classOf[HashAggregateExec] -> CometHashAggregateExec,
      classOf[ObjectHashAggregateExec] -> CometObjectHashAggregateExec,
      classOf[BroadcastHashJoinExec] -> CometBroadcastHashJoinExec,
      classOf[ShuffledHashJoinExec] -> CometHashJoinExec,
      classOf[SortMergeJoinExec] -> CometSortMergeJoinExec,
      classOf[SortExec] -> CometSortExec,
      classOf[LocalTableScanExec] -> CometLocalTableScanExec,
      classOf[WindowExec] -> CometWindowExec)

  /**
   * Sinks that have a native plan of ScanExec.
   */
  val sinks: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(
      classOf[CoalesceExec] -> CometCoalesceExec,
      classOf[CollectLimitExec] -> CometCollectLimitExec,
      classOf[TakeOrderedAndProjectExec] -> CometTakeOrderedAndProjectExec,
      classOf[UnionExec] -> CometUnionExec)

  val allExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] = nativeExecs ++ sinks

  /**
   * Tag set on a `ShuffleExchangeExec` that should be left as a plain Spark shuffle rather than
   * wrapped in `CometShuffleExchangeExec`. See `tagRedundantColumnarShuffle`.
   */
  val SKIP_COMET_SHUFFLE_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometShuffle")

  /**
   * Tag set on a `BroadcastExchangeExec` that should be left as a plain Spark broadcast rather
   * than converted to `CometBroadcastExchangeExec`. Written by [[CometSpark34AqeDppFallbackRule]]
   * on Spark < 3.5. See that rule's class docstring for the rationale.
   */
  val SKIP_COMET_BROADCAST_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometBroadcast")
}

/**
 * Spark physical optimizer rule for replacing Spark operators with Comet operators.
 */
case class CometExecRule(session: SparkSession)
    extends Rule[SparkPlan]
    with ShimSubqueryBroadcast {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  /**
   * Revert any `CometShuffleExchangeExec` with `CometColumnarShuffle` whose parent and child are
   * both non-Comet `HashAggregateExec` / `ObjectHashAggregateExec` operators back to the original
   * Spark `ShuffleExchangeExec`. This is the partial-final-aggregate pattern where Comet couldn't
   * convert either aggregate; keeping a columnar shuffle between them only adds
   * row->arrow->shuffle->arrow->row conversion overhead with no Comet consumer on either side.
   * See https://github.com/apache/datafusion-comet/issues/4004.
   *
   * The match is intentionally narrow (both sides must be row-based aggregates that remained JVM
   * after the main transform pass). Running the revert post-transform means we only fire when the
   * main conversion already decided to keep both aggregates JVM - we never create the dangerous
   * mixed mode where a Comet partial feeds a JVM final (see issue #1389).
   *
   * Correctness depends on running as part of `preColumnarTransitions`: if the revert ran after
   * Spark inserted `ColumnarToRowExec` between the aggregate and the columnar shuffle, the
   * pattern would no longer match (the shuffle would be separated from the aggregate by the
   * transition) and the unnecessary conversion could not be eliminated.
   *
   * The reverted shuffle is tagged with `SKIP_COMET_SHUFFLE_TAG` so both the AQE
   * `QueryStagePrepRule` pass and the `ColumnarRule` `preColumnarTransitions` pass leave it alone
   * on re-entry - AQE in particular re-runs the rule on each stage in isolation, where the outer
   * aggregate context is no longer visible and the shuffle would otherwise be re-wrapped as a
   * Comet columnar shuffle.
   */
  private def revertRedundantColumnarShuffle(plan: SparkPlan): SparkPlan = {
    // A JVM aggregate that contains geo in its resultExpressions is NOT a pure JVM op:
    // CometExecRule will convert it via the geo extraction path. Do not revert the shuffle.
    def isJvmOnlyAggregate(p: SparkPlan): Boolean = p match {
      case agg: HashAggregateExec =>
        !CometGeoExtractFromAggRule.hasGeoInResults(agg)
      case _: ObjectHashAggregateExec => true
      case _ => false
    }

    def isRedundantShuffle(child: SparkPlan): Boolean = child match {
      case s: CometShuffleExchangeExec =>
        s.shuffleType == CometColumnarShuffle && isJvmOnlyAggregate(s.child)
      case _ => false
    }

    plan.transform {
      case op if isJvmOnlyAggregate(op) && op.children.exists(isRedundantShuffle) =>
        val newChildren = op.children.map {
          case s: CometShuffleExchangeExec
              if s.shuffleType == CometColumnarShuffle && isJvmOnlyAggregate(s.child) =>
            val reverted =
              s.originalPlan.withNewChildren(Seq(s.child)).asInstanceOf[ShuffleExchangeExec]
            reverted.setTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG, ())
            logInfo(
              "Reverting Comet columnar shuffle to Spark shuffle between " +
                s"${op.getClass.getSimpleName} and ${s.child.getClass.getSimpleName} " +
                "(no Comet operator on either side to consume columnar output)")
            reverted
          case other => other
        }
        op.withNewChildren(newChildren)
    }
  }

  private def shouldSkipCometShuffle(s: ShuffleExchangeExec): Boolean =
    s.getTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG).isDefined

  private def applyCometShuffle(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case s: ShuffleExchangeExec if shouldSkipCometShuffle(s) =>
        s
      case s: ShuffleExchangeExec =>
        CometShuffleExchangeExec.shuffleSupported(s) match {
          case Some(CometNativeShuffle) =>
            CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
          case Some(CometColumnarShuffle) =>
            CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
          case None =>
            s
        }
    }
  }

  private def isCometNative(op: SparkPlan): Boolean = op.isInstanceOf[CometNativeExec]

  // spotless:off

  /**
   * Tries to transform a Spark physical plan into a Comet plan.
   *
   * This rule traverses bottom-up from the original Spark plan and for each plan node, there are
   * a few cases to consider:
   *
   *   1. The child(ren) of the current node `p` cannot be converted to native In this case, we'll
   *      simply return the original Spark plan, since Comet native execution cannot start from an
   *      arbitrary Spark operator (unless it is special node such as scan or sink such as shuffle
   *      exchange, union etc., which are wrapped by `CometScanWrapper` and `CometSinkPlaceHolder`
   *      respectively).
   *
   * 2. The child(ren) of the current node `p` can be converted to native There are two sub-cases
   * for this scenario: 1) This node `p` can also be converted to native. In this case, we'll
   * create a new native Comet operator for `p` and connect it with its previously converted
   * child(ren); 2) This node `p` cannot be converted to native. In this case, similar to 1)
   * above, we simply return `p` as it is. Its child(ren) would still be native Comet operators.
   *
   * After this rule finishes, we'll do another pass on the final plan to convert all adjacent
   * Comet native operators into a single native execution block. Please see where `convertBlock`
   * is called below.
   *
   * Here are a few examples:
   *
   * Scan ======> CometScan \| | Filter CometFilter \| | HashAggregate CometHashAggregate \| |
   * Exchange CometExchange \| | HashAggregate CometHashAggregate \| | UnsupportedOperator
   * UnsupportedOperator
   *
   * Native execution doesn't necessarily have to start from `CometScan`:
   *
   * Scan =======> CometScan \| | UnsupportedOperator UnsupportedOperator \| | HashAggregate
   * HashAggregate \| | Exchange CometExchange \| | HashAggregate CometHashAggregate \| |
   * UnsupportedOperator UnsupportedOperator
   *
   * A sink can also be Comet operators other than `CometExchange`, for instance `CometUnion`:
   *
   * Scan Scan =======> CometScan CometScan \| | | | Filter Filter CometFilter CometFilter \| | |
   * \| Union CometUnion \| | Project CometProject
   */
  // spotless:on
  private def transform(plan: SparkPlan): SparkPlan = {
    def convertNode(op: SparkPlan): SparkPlan = op match {
      // Fully native scan for V1. CometScanExec must always convert to a native scan; the JVM
      // fallback path has been removed. If conversion fails, fall back to the original Spark scan.
      case scan: CometScanExec =>
        convertToComet(scan, CometNativeScan).getOrElse(scan.wrapped)

      // Fully native Iceberg scan for V2 (iceberg-rust path)
      // Only handle scans with native metadata; other scans fall through to isCometScan
      // Config checks (COMET_ICEBERG_NATIVE_ENABLED, COMET_EXEC_ENABLED) are done in CometScanRule
      case scan: CometBatchScanExec if scan.nativeIcebergScanMetadata.isDefined =>
        convertToComet(scan, CometIcebergNativeScan).getOrElse(scan)

      case scan: CometBatchScanExec if scan.wrapped.scan.isInstanceOf[CSVScan] =>
        convertToComet(scan, CometCsvNativeScanExec).getOrElse(scan)

      // Comet JVM + native scan for V1 and V2
      case op if isCometScan(op) =>
        convertToComet(op, CometScanWrapper).getOrElse(op)

      case op if shouldApplySparkToColumnar(conf, op) =>
        convertToComet(op, CometSparkToColumnarExec).getOrElse(op)

      // AQE reoptimization looks for `DataWritingCommandExec` or `WriteFilesExec`
      // if there is none it would reinsert write nodes, and since Comet remap those nodes
      // to Comet counterparties the write nodes are twice to the plan.
      // Checking if AQE inserted another write Command on top of existing write command
      case _ @DataWritingCommandExec(_, w: WriteFilesExec)
          if w.child.isInstanceOf[CometNativeWriteExec] =>
        w.child

      case op: DataWritingCommandExec =>
        convertToComet(op, CometDataWritingCommand).getOrElse(op)

      // For AQE broadcast stage on a Comet broadcast exchange
      case s @ BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      case s @ BroadcastQueryStageExec(
            _,
            ReusedExchangeExec(_, _: CometBroadcastExchangeExec),
            _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // `CometBroadcastExchangeExec`'s broadcast output is not compatible with Spark's broadcast
      // exchange. It is only used for Comet native execution. We only transform Spark broadcast
      // exchange to Comet broadcast exchange if its downstream is a Comet native plan or if the
      // broadcast exchange is forced to be enabled by Comet config.
      case plan if plan.children.exists(_.isInstanceOf[BroadcastExchangeExec]) =>
        val newChildren = plan.children.map {
          // Tagged by CometSpark34AqeDppFallbackRule on Spark < 3.5 to keep the build-side
          // broadcast Spark-native so Spark's PlanAdaptiveDynamicPruningFilters can match it.
          case b: BroadcastExchangeExec
              if b.getTagValue(CometExecRule.SKIP_COMET_BROADCAST_TAG).isDefined =>
            b
          case b: BroadcastExchangeExec if b.children.forall(_.isInstanceOf[CometNativeExec]) =>
            convertToComet(b, CometBroadcastExchangeExec).getOrElse(b)
          case other => other
        }
        if (!newChildren.exists(_.isInstanceOf[BroadcastExchangeExec])) {
          val newPlan = convertNode(plan.withNewChildren(newChildren))
          if (isCometNative(newPlan) || CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.get(conf)) {
            newPlan
          } else {
            // copy fallback reasons to the original plan
            newPlan
              .getTagValue(CometExplainInfo.EXTENSION_INFO)
              .foreach(reasons => withInfos(plan, reasons))
            // return the original plan
            plan
          }
        } else {
          plan
        }

      // For AQE shuffle stage on a Comet shuffle exchange
      case s @ ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // For AQE shuffle stage on a reused Comet shuffle exchange
      // Note that we don't need to handle `ReusedExchangeExec` for non-AQE case, because
      // the query plan won't be re-optimized/planned in non-AQE mode.
      case s @ ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: CometShuffleExchangeExec), _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // For the final AQE re-plan: AQEShuffleReadExec wraps a ShuffleQueryStageExec that
      // transformUp has already converted to CometExchangeSink (a CometNativeExec). Convert the
      // AQEShuffleReadExec itself to a CometExchangeSink (as a native leaf) so that operators above
      // it (e.g. a geo-bearing HashAggregateExec) see an all-native child and proceed through the
      // normal convertToComet geo-extraction path. foreachUntilCometInput already treats
      // AQEShuffleReadExec as a native input source so this is safe at execution time.
      case r: AQEShuffleReadExec if r.child.isInstanceOf[CometNativeExec] =>
        convertToComet(r, CometExchangeSink).getOrElse(r)

      case s: ShuffleExchangeExec if shouldSkipCometShuffle(s) =>
        s

      case s: ShuffleExchangeExec =>
        convertToComet(s, CometShuffleExchangeExec).getOrElse(s)

      case op =>
        // Try to wrap any JVM children in CometSparkToColumnarExec so that the parent
        // operator sees all-native children. This is the re-entry mechanism: when a JVM
        // operator sits between two native blocks, we bridge it with the same
        // CometScanWrapper(CometSparkToColumnarExec(jvmChild)) pattern already used for
        // leaf scans (shouldApplySparkToColumnar). foreachUntilCometInput already
        // recognises CometSparkToColumnarExec as a valid native input source, so the
        // native execution block picks up the Arrow batches it produces.
        //
        // This is identical to how Gluten uses InputIteratorTransformer as the leaf of a
        // re-entered WholeStageTransformer fragment.

        // Geo-bearing Final HashAggregateExec: intercept before the all-native-children check.
        // In AQE re-planning the child is AQEShuffleRead (not CometNativeExec), so the normal
        // forall-native path never fires. convertToComet will either convert natively (good
        // path) or apply the safe-null fallback to prevent CometGeoFallback.notSupported().
        op match {
          case agg: HashAggregateExec if CometGeoExtractFromAggRule.hasGeoInResults(agg) =>
            return convertToComet(agg, CometHashAggregateExec).getOrElse(safeGeoFallback(op))
          case _ =>
        }

        // ProjectExec: rewrite ToPrettyString(geo) -> StAsText(geo) before serde so that
        // show()-style toprettystring wrappers serialize natively as WKT strings.
        val geoRewrittenOp: SparkPlan = op match {
          case proj: ProjectExec
              if proj.projectList.exists(e =>
                CometGeoExtractFromAggRule.containsGeoExpr(
                  CometGeoExtractFromAggRule.unwrapAlias(e))) =>
            val rewritten = proj.projectList.map { e =>
              CometGeoExtractFromAggRule
                .replacePrettyGeoToAsText(e)
                .asInstanceOf[NamedExpression]
            }
            if (rewritten != proj.projectList) proj.copy(projectList = rewritten) else proj
          case other => other
        }

        val opWithBridgedChildren = wrapJvmChildrenIfSafe(geoRewrittenOp)

        // Now all children are either originally-native or freshly-bridged.
        // Only attempt operator conversion when all children ended up native.
        val allNative = opWithBridgedChildren.children.forall(_.isInstanceOf[CometNativeExec])
        if (allNative) {
          val handler = allExecs
            .get(opWithBridgedChildren.getClass)
            .map(_.asInstanceOf[CometOperatorSerde[SparkPlan]])
          handler match {
            case Some(handler) =>
              val result = convertToComet(opWithBridgedChildren, handler)
              opWithBridgedChildren match {
                case p: ProjectExec
                    if p.projectList.exists(e =>
                      CometGeoExtractFromAggRule.containsGeoExpr(
                        CometGeoExtractFromAggRule.unwrapAlias(e))) =>
                  val exprResults = p.projectList.map { e =>
                    val r = QueryPlanSerde.exprToProto(e, p.child.output)
                    s"${e.name}=${r.isDefined}(${e.getClass.getSimpleName})"
                  }
                  logInfo(
                    s"[GeoProjConvert] result=${result.isDefined}" +
                      s" exprs=${exprResults.mkString(",")}")
                case _ =>
              }
              // If native conversion succeeded, use it. Otherwise for a geo-bearing ProjectExec
              // that falls through to JVM, replace geo exprs with safe literals so JVM codegen
              // doesn't call geo.eval() -> CometGeoFallback.notSupported() crash.
              return result.getOrElse(safeGeoFallback(geoRewrittenOp))
            case _ =>
          }
        }

        // children were not all-native (e.g. Window above AQEShuffleRead): still must not let
        // geo exprs reach JVM codegen. Apply safe-literal fallback for any geo ProjectExec.
        val safeOp = safeGeoFallback(geoRewrittenOp)
        safeOp match {
          case _: CometPlan | _: AQEShuffleReadExec | _: BroadcastExchangeExec |
              _: BroadcastQueryStageExec | _: AdaptiveSparkPlanExec | _: ExecutedCommandExec |
              _: V2CommandExec =>
            // Some execs should never be replaced. We include
            // these cases specially here so we do not add a misleading 'info' message
            safeOp
          case _ =>
            // The operator was not converted to a Comet plan. Possible reasons:
            // 1. Comet does not support this operator.
            // 2. The operator could not be supported based on query context / configs.
            //    It should already be tagged with fallback reasons.
            // 3. Some children could not be bridged (unsafe type, tagged, excluded node).
            if (opWithBridgedChildren.children.forall(_.isInstanceOf[CometNativeExec])
              && !hasExplainInfo(safeOp)) {
              withInfo(safeOp, s"${safeOp.nodeName} is not supported")
            } else {
              safeOp
            }
        }
    }

    plan.transformUp { case op =>
      val converted = convertNode(op)
      // Replace SubqueryBroadcastExec with CometSubqueryBroadcastExec in DPP expressions
      // when the broadcast child has a Comet plan underneath. This enables exchange reuse
      // between the DPP subquery and the join's CometBroadcastExchangeExec because both
      // will have the same CometBroadcastExchangeExec type and canonical form.
      convertSubqueryBroadcasts(converted)
    }
  }

  /**
   * Replace SubqueryBroadcastExec with CometSubqueryBroadcastExec in a node's expressions
   * (non-AQE DPP), and wrap SubqueryAdaptiveBroadcastExec in CometSubqueryAdaptiveBroadcastExec
   * (AQE DPP) to protect it from Spark's PlanAdaptiveDynamicPruningFilters.
   *
   * Non-AQE DPP: When CometExecRule converts BroadcastExchangeExec to CometBroadcastExchangeExec
   * on the join side, the DPP subquery still references the original BroadcastExchangeExec.
   * ReuseExchangeAndSubquery (which runs after Comet rules) can't match them because they have
   * different types. By replacing SubqueryBroadcastExec with CometSubqueryBroadcastExec (which
   * wraps a CometBroadcastExchangeExec), both sides have the same exchange type and reuse works.
   *
   * AQE DPP: Spark's PlanAdaptiveDynamicPruningFilters (queryStageOptimizerRule) pattern-matches
   * on SubqueryAdaptiveBroadcastExec. When it can't find BroadcastHashJoinExec (Comet replaced
   * it), it replaces DPP with Literal.TrueLiteral. We wrap SABs in
   * CometSubqueryAdaptiveBroadcastExec to prevent this. CometPlanAdaptiveDynamicPruningFilters (a
   * later queryStageOptimizerRule) unwraps and converts them with access to the materialized
   * BroadcastQueryStageExec.
   */
  private def convertSubqueryBroadcasts(plan: SparkPlan): SparkPlan = {
    // CometIcebergNativeScanExec.runtimeFilters is a top-level constructor field visible to
    // productIterator, so transformExpressionsUp rewrites it directly. The wrapped @transient
    // originalPlan still holds the pre-rewrite runtimeFilters; we don't sync it here because
    // CometIcebergNativeScanExec.serializedPartitionData rebuilds originalPlan from the
    // top-level runtimeFilters at serialization time (single source of truth).
    plan.transformExpressionsUp { case inSub: InSubqueryExec =>
      rewriteInSubqueryPlan(inSub)
    }
  }

  private def rewriteInSubqueryPlan(inSub: InSubqueryExec): Expression = {
    inSub.plan match {
      case sub: SubqueryBroadcastExec =>
        sub.child match {
          case b: BroadcastExchangeExec =>
            // The BroadcastExchangeExec child is CometNativeColumnarToRowExec wrapping
            // a Comet plan. Strip the row transition to get the columnar Comet plan.
            val cometChild = b.child match {
              case c2r: CometNativeColumnarToRowExec => c2r.child
              case other => other
            }
            if (cometChild.isInstanceOf[CometNativeExec]) {
              logInfo(
                "Converting SubqueryBroadcastExec to " +
                  "CometSubqueryBroadcastExec for DPP exchange reuse")
              val cometBroadcast = CometBroadcastExchangeExec(b, b.output, b.mode, cometChild)
              val cometSub = CometSubqueryBroadcastExec(
                sub.name,
                getSubqueryBroadcastExecIndices(sub),
                sub.buildKeys,
                cometBroadcast)
              inSub.withNewPlan(cometSub)
            } else {
              inSub
            }
          case _ => inSub
        }
      case sab: SubqueryAdaptiveBroadcastExec if isSpark35Plus =>
        // Wrap SABs to prevent Spark's PlanAdaptiveDynamicPruningFilters from
        // converting them to Literal.TrueLiteral. Spark's rule pattern-matches for
        // BroadcastHashJoinExec, which Comet replaced with CometBroadcastHashJoinExec.
        // Without wrapping, DPP is disabled for both Comet native scans and non-Comet
        // scans (e.g., V2 BatchScan). CometPlanAdaptiveDynamicPruningFilters
        // (queryStageOptimizerRule, 3.5+) unwraps and converts them later.
        //
        // On Spark 3.4, injectQueryStageOptimizerRule is unavailable. The isSpark35Plus
        // guard leaves SABs unwrapped; CometSpark34AqeDppFallbackRule then tags the
        // matching BHJ's build broadcast so Spark's rule can match it natively.
        assert(
          sab.buildKeys.nonEmpty,
          s"SubqueryAdaptiveBroadcastExec '${sab.name}' has empty buildKeys")
        logInfo(
          s"Wrapping SubqueryAdaptiveBroadcastExec '${sab.name}' in " +
            "CometSubqueryAdaptiveBroadcastExec to preserve AQE DPP")
        val indices = getSubqueryBroadcastIndices(sab)
        val wrapped = CometSubqueryAdaptiveBroadcastExec(
          sab.name,
          indices,
          sab.onlyInBroadcast,
          sab.buildPlan,
          sab.buildKeys,
          sab.child)
        inSub.withNewPlan(wrapped)
      case _ => inSub
    }
  }

  private def normalizePlan(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p: ProjectExec =>
        val newProjectList = p.projectList.map(normalize(_).asInstanceOf[NamedExpression])
        ProjectExec(newProjectList, p.child)
      case f: FilterExec =>
        val newCondition = normalize(f.condition)
        FilterExec(newCondition, f.child)
    }
  }

  // Spark will normalize NaN and zero for floating point numbers for several cases.
  // See `NormalizeFloatingNumbers` optimization rule in Spark.
  // However, one exception is for comparison operators. Spark does not normalize NaN and zero
  // because they are handled well in Spark (e.g., `SQLOrderingUtil.compareFloats`). But the
  // comparison functions in arrow-rs do not normalize NaN and zero. So we need to normalize NaN
  // and zero for comparison operators in Comet.
  private def normalize(expr: Expression): Expression = {
    expr.transformUp {
      case EqualTo(left, right) =>
        EqualTo(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case EqualNullSafe(left, right) =>
        EqualNullSafe(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case GreaterThan(left, right) =>
        GreaterThan(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case GreaterThanOrEqual(left, right) =>
        GreaterThanOrEqual(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case LessThan(left, right) =>
        LessThan(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case LessThanOrEqual(left, right) =>
        LessThanOrEqual(normalizeNaNAndZero(left), normalizeNaNAndZero(right))
      case Divide(left, right, evalMode) =>
        Divide(left, normalizeNaNAndZero(right), evalMode)
      case Remainder(left, right, evalMode) =>
        Remainder(left, normalizeNaNAndZero(right), evalMode)
    }
  }

  private def normalizeNaNAndZero(expr: Expression): Expression = {
    expr match {
      case _: KnownFloatingPointNormalized => expr
      case FloatLiteral(f) if !f.equals(-0.0f) => expr
      case DoubleLiteral(d) if !d.equals(-0.0d) => expr
      case _ =>
        expr.dataType match {
          case _: FloatType | _: DoubleType =>
            KnownFloatingPointNormalized(NormalizeNaNAndZero(expr))
          case _ => expr
        }
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = _apply(plan)
    if (showTransformations && !newPlan.fastEquals(plan)) {
      logInfo(s"""
           |=== Applying Rule $ruleName ===
           |${sideBySide(plan.treeString, newPlan.treeString).mkString("\n")}
           |""".stripMargin)
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    // We shouldn't transform Spark query plan if Comet is not loaded.
    if (!isCometLoaded(conf)) return plan

    // Comet does not support structured streaming. Fall back to Spark for any plan that
    // belongs to a streaming query (detected via StreamSourceAwareSparkPlan.getStream).
    if (ShimCometStreaming.isStreamingPlan(plan)) return plan

    if (!CometConf.COMET_EXEC_ENABLED.get(conf)) {
      // Comet exec is disabled, but for Spark shuffle, we still can use Comet columnar shuffle
      if (isCometShuffleEnabled(conf)) {
        applyCometShuffle(plan)
      } else {
        plan
      }
    } else {
      val normalizedPlan = normalizePlan(plan)

      val planWithJoinRewritten = if (CometConf.COMET_REPLACE_SMJ.get()) {
        normalizedPlan.transformUp { case p =>
          RewriteJoin.rewrite(p)
        }
      } else {
        normalizedPlan
      }

      // Tag Partial aggregates that must not be converted to Comet because the
      // corresponding Final aggregate cannot be converted and the intermediate buffer
      // formats are incompatible. This runs before transform() so the tags are checked
      // during the bottom-up conversion. Tags persist through AQE stage creation.
      tagUnsafePartialAggregates(planWithJoinRewritten)

      var newPlan = transform(planWithJoinRewritten)

      // if the plan cannot be run fully natively then explain why (when appropriate
      // config is enabled)
      if (CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.get()) {
        val info = new ExtendedExplainInfo()
        if (info.extensionInfo(newPlan).nonEmpty) {
          logWarning(
            "Comet cannot execute some parts of this plan natively " +
              s"(set ${CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key}=false " +
              "to disable this logging):\n" +
              s"${info.generateExtendedInfo(newPlan)}")
        }
      }

      // Remove placeholders
      newPlan = newPlan.transform {
        case CometSinkPlaceHolder(_, _, s) => s
        case CometScanWrapper(_, s) => s
      }

      // Revert CometColumnarShuffle to Spark's ShuffleExchangeExec when both its parent and child
      // are non-Comet HashAggregate/ObjectHashAggregate operators that remained JVM after the main
      // transform pass. See https://github.com/apache/datafusion-comet/issues/4004.
      if (CometConf.COMET_EXEC_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.get()) {
        logInfo(s"[GeoRevert] plan before revert:\n${newPlan.treeString}")
        newPlan = revertRedundantColumnarShuffle(newPlan)
      }

      // Set up logical links
      newPlan = newPlan.transform {
        case op: CometExec =>
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op
        case op: CometShuffleExchangeExec =>
          // Original Spark shuffle exchange operator might have empty logical link.
          // But the `setLogicalLink` call above on downstream operator of
          // `CometShuffleExchangeExec` will set its logical link to the downstream
          // operators which cause AQE behavior to be incorrect. So we need to unset
          // the logical link here.
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op

        case op: CometBroadcastExchangeExec =>
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op
      }

      // Convert native execution block by linking consecutive native operators.
      var firstNativeOp = true
      newPlan.transformDown {
        case op: CometNativeExec =>
          val newPlan = if (firstNativeOp) {
            firstNativeOp = false
            op.convertBlock()
          } else {
            op
          }

          // If reaching leaf node, reset `firstNativeOp` to true
          // because it will start a new block in next iteration.
          if (op.children.isEmpty) {
            firstNativeOp = true
          }

          // CometNativeWriteExec is special: it has two separate plans:
          // 1. A protobuf plan (nativeOp) describing the write operation
          // 2. A Spark plan (child) that produces the data to write
          // The serializedPlanOpt is a def that always returns Some(...) by serializing
          // nativeOp on-demand, so it doesn't need convertBlock(). However, its child
          // (e.g., CometNativeScanExec) may need its own serialization. Reset the flag
          // so children can start their own native execution blocks.
          if (op.isInstanceOf[CometNativeWriteExec]) {
            firstNativeOp = true
          }

          newPlan
        case op =>
          firstNativeOp = true
          op
      }
    }
  }

  /** Convert a Spark plan to a Comet plan using the specified serde handler */
  private def convertToComet(op: SparkPlan, handler: CometOperatorSerde[_]): Option[SparkPlan] = {
    val serde = handler.asInstanceOf[CometOperatorSerde[SparkPlan]]
    op match {
      case h: HashAggregateExec if CometGeoExtractFromAggRule.hasGeoInResults(h) =>
        val childNative = h.children.forall(_.isInstanceOf[CometNativeExec])
        logInfo(
          s"[GeoEntry] convertToComet geo agg handler=${handler.getClass.getSimpleName}" +
            s" childNative=$childNative child=${h.children.head.getClass.getSimpleName}")
      case _ =>
    }
    if (isOperatorEnabled(serde, op)) {
      // For operators that require native children (like writes), check if all data-producing
      // children are CometNativeExec. This prevents runtime failures when the native operator
      // expects Arrow arrays but receives non-Arrow data (e.g., OnHeapColumnVector).
      if (serde.requiresNativeChildren && op.children.nonEmpty) {
        // Get the actual data-producing children (unwrap WriteFilesExec if present)
        val dataProducingChildren = op.children.flatMap {
          case writeFiles: WriteFilesExec => Seq(writeFiles.child)
          case other => Seq(other)
        }
        if (!dataProducingChildren.forall(_.isInstanceOf[CometNativeExec])) {
          withInfo(op, "Cannot perform native operation because input is not in Arrow format")
          return None
        }
      }

      val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(op.id)
      if (op.children.nonEmpty && op.children.forall(_.isInstanceOf[CometNativeExec])) {
        val childOp = op.children.map(_.asInstanceOf[CometNativeExec].nativeOp)
        childOp.foreach(builder.addChildren)
        // If a Final HashAggregateExec has geo expressions in resultExpressions, strip them
        // before serde (geo cannot be serialized against aggregateAttributes), convert the
        // stripped aggregate natively, then build a CometProjectExec above it that re-applies
        // the geo exprs bound against the stripped agg's output attributes.
        op match {
          case agg: HashAggregateExec if CometGeoExtractFromAggRule.hasGeoInResults(agg) =>
            val (stripped, geoProjectList) =
              CometGeoExtractFromAggRule.stripGeoFromResults(agg)
            val strippedBuilder =
              OperatorOuterClass.Operator.newBuilder().setPlanId(stripped.id)
            childOp.foreach(strippedBuilder.addChildren)
            val aggSerde = serde.asInstanceOf[CometOperatorSerde[HashAggregateExec]]
            val aggNativeOpt = aggSerde.convert(stripped, strippedBuilder, childOp: _*)
            val childName = op.children.head.getClass.getSimpleName
            val strippedNames = stripped.resultExpressions.map(_.name).mkString(",")
            logInfo(
              s"[GeoAgg] native path: aggNativeOpt=${aggNativeOpt.isDefined}" +
                s" child=$childName strippedResults=$strippedNames")
            aggNativeOpt match {
              case Some(aggNativeOp) =>
                val cometAgg = aggSerde.createExec(aggNativeOp, stripped)
                val projExec = ProjectExec(geoProjectList, cometAgg)
                val projBuilder =
                  OperatorOuterClass.Operator.newBuilder().setPlanId(projExec.id)
                val projResult = CometProjectExec
                  .convert(projExec, projBuilder, aggNativeOp)
                  .map(projNativeOp => CometProjectExec.createExec(projNativeOp, projExec))
                val geoNames = geoProjectList.map(_.name).mkString(",")
                logInfo(s"[GeoAgg] projResult=${projResult.isDefined} geoList=$geoNames")
                if (projResult.isDefined) return projResult
                // Geo projection serde failed - return None so AQE can re-plan with safe-null.
                return None
              case None =>
                // Stripped agg serde failed - return None so AQE can re-plan with safe-null.
                return None
            }
          case _ =>
        }
        val nativeResult = serde
          .convert(op, builder, childOp: _*)
          .map(nativeOp => serde.createExec(nativeOp, op))
        if (nativeResult.isDefined) return nativeResult
      } else {
        val nativeResult = serde
          .convert(op, builder)
          .map(nativeOp => serde.createExec(nativeOp, op))
        if (nativeResult.isDefined) return nativeResult
        // Native children path failed for a geo-bearing agg (e.g. AQE final stage where
        // child is AQEShuffleRead). Replace geo result exprs with safe literals so the JVM
        // agg runs without calling st_point.eval() -> CometGeoFallback.notSupported() crash.
        op match {
          case agg: HashAggregateExec if CometGeoExtractFromAggRule.hasGeoInResults(agg) =>
            val safeResultExprs = agg.resultExpressions.map {
              case e
                  if CometGeoExtractFromAggRule.containsGeoExpr(
                    CometGeoExtractFromAggRule.unwrapAlias(e)) =>
                // toprettystring output is StringType NOT NULL -- use "" not null to avoid
                // NPE in show() deserialization. For other types (binary geo) use null.
                val safeLit =
                  if (e.dataType == StringType) Literal("") else Literal(null, e.dataType)
                Alias(safeLit, e.name)(e.exprId, e.qualifier)
              case e => e
            }
            logInfo(
              s"[GeoAgg] safe-null fallback for geo agg" +
                s" (AQE final stage, replacing geo results with null)")
            return Some(agg.copy(resultExpressions = safeResultExprs))
          case _ =>
        }
      }
    }
    None
  }

  private def isOperatorEnabled(
      handler: CometOperatorSerde[SparkPlan],
      op: SparkPlan): Boolean = {
    val opName = op.getClass.getSimpleName
    if (handler.enabledConfig.forall(_.get(op.conf))) {
      handler.getSupportLevel(op) match {
        case Unsupported(notes) =>
          withInfo(op, notes.getOrElse(""))
          false
        case Incompatible(notes) =>
          val allowIncompat = CometConf.isOperatorAllowIncompat(opName)
          val incompatConf = CometConf.getOperatorAllowIncompatConfigKey(opName)
          if (allowIncompat) {
            if (notes.isDefined) {
              logWarning(
                s"Comet supports $opName when $incompatConf=true " +
                  s"but has notes: ${notes.get}")
            }
            true
          } else {
            val optionalNotes = notes.map(str => s" ($str)").getOrElse("")
            withInfo(
              op,
              s"$opName is not fully compatible with Spark$optionalNotes. " +
                s"To enable it anyway, set $incompatConf=true. " +
                s"${CometConf.COMPAT_GUIDE}.")
            false
          }
        case Compatible(notes) =>
          if (notes.isDefined) {
            logWarning(s"Comet supports $opName but has notes: ${notes.get}")
          }
          true
      }
    } else {
      withInfo(
        op,
        s"Native support for operator $opName is disabled. " +
          s"Set ${handler.enabledConfig.get.key}=true to enable it.")
      false
    }
  }

  /**
   * Replace every geo sub-expression in `plan`'s expressions with a type-safe literal so that JVM
   * codegen never calls geo.eval() -> CometGeoFallback.notSupported(). Applies to ALL operator
   * types (ProjectExec, FilterExec, SortExec, HashAggregateExec, etc.).
   *
   * Replacement rules:
   *   - A geo expr whose parent is Alias: the whole Alias gets a safe-literal child.
   *   - A bare geo expr (no Alias wrapper, e.g. in a Filter predicate): replaced directly.
   *   - StringType result -> Literal("") (toprettystring output is NOT NULL).
   *   - Boolean predicates (st_contains, st_intersects, ...) -> Literal(true) so filters pass.
   *   - Other types (BinaryType geo WKB, DoubleType measurements) -> Literal(null, dataType).
   *
   * Returns the plan unchanged if it carries no geo expressions.
   */
  private def safeGeoFallback(plan: SparkPlan): SparkPlan = {
    val hasGeo = plan.expressions.exists(CometGeoExtractFromAggRule.containsGeoExpr)
    if (!hasGeo) return plan
    logInfo(s"[GeoRevertFallback] replacing geo exprs with safe literals in ${plan.nodeName}")
    // transformExpressions is bottom-up: inner geo nodes are replaced first.
    // CometGeoExpression nodes become safe literals; Alias wrappers are untouched
    // (they still wrap the safe literal, preserving exprId/qualifier).
    // Boolean predicates (st_contains etc.) get Literal(true) so filters keep passing.
    plan.transformExpressions { case e: CometGeoExpression =>
      if (e.dataType == StringType) Literal("")
      else if (e.dataType == BooleanType) Literal(true)
      else Literal(null, e.dataType)
    }
  }

  /**
   * For each JVM (non-native) child of `op`, attempt to wrap it in
   * `CometScanWrapper(CometSparkToColumnarExec(child))` - a leaf `CometNativeExec` that bridges
   * the JVM row/columnar output into Arrow `ColumnarBatch` for the parent.
   *
   * This is the Comet equivalent of Gluten's `InputIteratorTransformer` leaf: the native parent
   * operator sees a valid `CometNativeExec` child and the `CometSink.convert()` produces a `Scan`
   * protobuf node (no `childOp` needed). At execution time `foreachUntilCometInput` recognises
   * `CometSparkToColumnarExec` as an input source and the native block reads Arrow batches from
   * it directly.
   *
   * Wrapping is skipped when the child:
   *   - is already a `CometNativeExec`
   *   - is a broadcast/shuffle/AQE exchange operator (different data protocol)
   *   - carries a `COMET_UNSAFE_PARTIAL` or `SKIP_COMET_SHUFFLE_TAG` tag
   *   - has a schema unsupported by `CometSparkToColumnarExec` (e.g. `CalendarIntervalType`)
   *
   * If wrapping fails (e.g. unsupported schema), the original child is left in place, so the
   * parent will not have all-native children and conversion will be skipped safely.
   */
  private def wrapJvmChildrenIfSafe(op: SparkPlan): SparkPlan = {
    if (op.children.isEmpty) return op
    if (op.children.forall(_.isInstanceOf[CometNativeExec])) return op

    val newChildren = op.children.map {
      case child: CometNativeExec => child
      case child if canWrapWithR2C(child) =>
        // Reuse the same CometSink path used by shouldApplySparkToColumnar for leaf scans:
        //   CometScanWrapper(nativeOp, CometSparkToColumnarExec(child))
        convertToComet(child, CometSparkToColumnarExec).getOrElse(child)
      case child => child
    }

    // Only rebuild the node when at least one child actually changed.
    if (newChildren.zip(op.children).exists { case (n, o) => n ne o }) {
      op.withNewChildren(newChildren)
    } else {
      op
    }
  }

  /**
   * Returns true when a JVM child plan can safely be wrapped in `CometSparkToColumnarExec` for
   * re-entry into native execution.
   */
  private def canWrapWithR2C(child: SparkPlan): Boolean = {
    // Validate schema first rejects CalendarIntervalType, UserDefinedType, etc.
    // collect_set / collect_list internal buffer arrays are caught by COMET_UNSAFE_PARTIAL above.
    val fallbackReasons = new ListBuffer[String]()
    if (!CometSparkToColumnarExec.isSchemaSupported(child.schema, fallbackReasons)) {
      return false
    }
    child match {
      // These operators cannot be wrapped: they don't produce standard rows/batches,
      // use different data protocols, or are already protected by other mechanisms.
      case _: BroadcastExchangeExec | _: BroadcastQueryStageExec | _: ReusedExchangeExec |
          _: ShuffleQueryStageExec | _: AQEShuffleReadExec | _: WriteFilesExec |
          _: DataWritingCommandExec =>
        false
      // Unsafe Partial Aggregate: Comet Partial -> Spark Final buffer mismatch.
      case c if c.getTagValue(CometExecRule.COMET_UNSAFE_PARTIAL).isDefined => false
      // Reverted Columnar Shuffle: already explicitly de-Cometed by AQE.
      case c if c.getTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG).isDefined => false
      // Already a CometPlan - don't double-wrap.
      case _: CometPlan => false
      // Geo Final aggregate: st_point has no JVM eval(). Wrapping it in CometSparkToColumnarExec
      // would cause TungstenAggregationIterator to call st_point.eval() -> CometGeoFallback crash.
      // These must go through convertToComet's geo extraction path, not the re-entry bridge.
      case h: HashAggregateExec if CometGeoExtractFromAggRule.hasGeoInResults(h) => false
      // Geo Project: any ProjectExec carrying a geo expression must go through the native
      // CometProjectExec path. Wrapping it would let JVM codegen call geo.eval() -> crash.
      case p: ProjectExec
          if p.projectList.exists(e =>
            CometGeoExtractFromAggRule.containsGeoExpr(
              CometGeoExtractFromAggRule.unwrapAlias(e))) =>
        false
      case _ => true
    }
  }

  private def shouldApplySparkToColumnar(conf: SQLConf, op: SparkPlan): Boolean = {
    // Only consider converting leaf nodes to columnar currently, so that all the following
    // operators can have a chance to be converted to columnar. Leaf operators that output
    // columnar batches, such as Spark's vectorized readers, will also be converted to native
    // comet batches.
    val fallbackReasons = new ListBuffer[String]()
    if (CometSparkToColumnarExec.isSchemaSupported(op.schema, fallbackReasons)) {
      op match {
        // Convert Spark DS v1 scan to Arrow format
        case scan: FileSourceScanExec =>
          scan.relation.fileFormat match {
            case _: CSVFileFormat => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonFileFormat => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetFileFormat => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _ => isSparkToArrowEnabled(conf, op)
          }
        // Convert Spark DS v2 scan to Arrow format
        case scan: BatchScanExec =>
          scan.scan match {
            case _: CSVScan => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonScan => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetScan => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _ => isSparkToArrowEnabled(conf, op)
          }
        // other leaf nodes
        case _: LeafExecNode =>
          isSparkToArrowEnabled(conf, op)
        case _ =>
          // TODO: consider converting other intermediate operators to columnar.
          false
      }
    } else {
      false
    }
  }

  private def isSparkToArrowEnabled(conf: SQLConf, op: SparkPlan) = {
    COMET_SPARK_TO_ARROW_ENABLED.get(conf) && {
      val simpleClassName = Utils.getSimpleName(op.getClass)
      val nodeName = simpleClassName.replaceAll("Exec$", "")
      COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.get(conf).contains(nodeName)
    }
  }

  /**
   * Walk the plan to find Final-mode aggregates that cannot be converted to Comet. For each such
   * Final, if the aggregate functions have incompatible intermediate buffer formats, tag the
   * corresponding Partial-mode aggregate so it will also be skipped during conversion.
   *
   * This prevents the crash described in issue #1389 where a Comet Partial produces intermediate
   * data in a format that the Spark Final cannot interpret.
   */
  private def tagUnsafePartialAggregates(plan: SparkPlan): Unit = {
    plan.foreach {
      case agg: BaseAggregateExec =>
        // Only consider single-mode Final aggregates. Multi-mode Finals come from Spark's
        // distinct-aggregate rewrite, where the Comet partial (if any) feeds into a Spark
        // PartialMerge rather than directly into a Final, which is a different code path
        // than the Comet-Partial -> Spark-Final crash scenario from issue #1389.
        val modes = agg.aggregateExpressions.map(_.mode).distinct
        if (modes == Seq(Final) &&
          !QueryPlanSerde.allAggsSupportMixedExecution(agg.aggregateExpressions) &&
          !canAggregateBeConverted(agg, Final)) {
          findPartialAggInPlan(agg.child).foreach { partial =>
            // Only tag if the Partial would otherwise have been converted. If the Partial
            // itself cannot be converted (e.g. the aggregate function is incompatible for the
            // input type), there is no buffer-format mismatch to guard against, and tagging
            // would mask the natural, more specific fallback reason.
            if (canAggregateBeConverted(partial, Partial)) {
              partial.setTagValue(
                CometExecRule.COMET_UNSAFE_PARTIAL,
                "Partial aggregate disabled: corresponding final aggregate " +
                  "cannot be converted to Comet and intermediate buffer formats are incompatible")
            }
          }
        }
      case _ =>
    }
  }

  /**
   * Conservative check for whether an aggregate could be converted to Comet. Checks operator
   * enablement, grouping expressions, aggregate expressions, and result expressions.
   * Intentionally skips the sparkFinalMode / child-native checks since those depend on
   * transformation state.
   *
   * WARNING: this intentionally mirrors the predicate checks in `CometBaseAggregate.doConvert`
   * (operators.scala). Any change to the convertibility rules there must be reflected here or
   * this tagging pass will drift and either crash (missed tag) or over-disable (spurious tag). A
   * shared predicate helper would be preferable.
   */
  private def canAggregateBeConverted(
      agg: BaseAggregateExec,
      expectedMode: AggregateMode): Boolean = {
    val handler = allExecs.get(agg.getClass)
    if (handler.isEmpty) return false
    val serde = handler.get.asInstanceOf[CometOperatorSerde[SparkPlan]]
    if (!isOperatorEnabled(serde, agg.asInstanceOf[SparkPlan])) return false

    // ObjectHashAggregate has an extra shuffle-enabled guard in its convert method
    agg match {
      case _: ObjectHashAggregateExec if !isCometShuffleEnabled(agg.conf) => return false
      case _ =>
    }

    val aggregateExpressions = agg.aggregateExpressions
    val groupingExpressions = agg.groupingExpressions

    if (groupingExpressions.isEmpty && aggregateExpressions.isEmpty) return false

    if (groupingExpressions.exists(e => QueryPlanSerde.containsMapType(e.dataType))) return false

    if (!groupingExpressions.forall(e =>
        QueryPlanSerde.exprToProto(e, agg.child.output).isDefined)) {
      return false
    }

    if (aggregateExpressions.isEmpty) {
      // Result expressions always checked when there are no aggregate expressions
      val attributes =
        groupingExpressions.map(_.toAttribute) ++ agg.aggregateAttributes
      return agg.resultExpressions.forall(e =>
        QueryPlanSerde.exprToProto(e, attributes).isDefined)
    }

    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.size != 1 || modes.head != expectedMode) return false

    // In Final mode, exprToProto resolves against the child's output; in Partial/non-Final mode
    // it must bind to input attributes. This mirrors the `binding` calculation in
    // `CometBaseAggregate.doConvert`.
    val binding = expectedMode != Final
    if (!aggregateExpressions.forall(e =>
        QueryPlanSerde.aggExprToProto(e, agg.child.output, binding, agg.conf).isDefined)) {
      return false
    }

    // doConvert only checks resultExpressions in Final mode when aggregate expressions exist
    // (Partial emits the buffer directly). Mirror that here to avoid false negatives.
    // Geo expressions in resultExpressions are handled by the geo extraction path in
    // convertToComet (stripped before serde, then re-applied as CometProjectExec above the
    // aggregate), so a Final agg is convertible even when geo exprs are present.
    if (expectedMode == Final) {
      agg match {
        case h: HashAggregateExec if CometGeoExtractFromAggRule.hasGeoInResults(h) => true
        case _ =>
          val attributes =
            groupingExpressions.map(_.toAttribute) ++ agg.aggregateAttributes
          agg.resultExpressions.forall(e => QueryPlanSerde.exprToProto(e, attributes).isDefined)
      }
    } else {
      true
    }
  }

  /**
   * Look for a Partial-mode aggregate that feeds directly into the given plan (the child of a
   * Final). Walks through exchanges and AQE stages only, stopping at anything else including
   * other aggregate stages. This avoids tagging unrelated Partials found deeper in the plan (e.g.
   * the non-distinct Partial in a distinct-aggregate rewrite, which is separated from the Final
   * by intermediate PartialMerge stages). Requires `aggregateExpressions.nonEmpty` so that
   * group-by-only dedup stages are not mistaken for the partial we want to tag.
   */
  private def findPartialAggInPlan(plan: SparkPlan): Option[BaseAggregateExec] = plan match {
    case agg: BaseAggregateExec
        if agg.aggregateExpressions.nonEmpty &&
          agg.aggregateExpressions.forall(e => e.mode == Partial) =>
      Some(agg)
    case a: AQEShuffleReadExec => findPartialAggInPlan(a.child)
    case s: ShuffleQueryStageExec => findPartialAggInPlan(s.plan)
    case e: ShuffleExchangeExec => findPartialAggInPlan(e.child)
    case other =>
      logDebug(s"findPartialAggInPlan: stopping at ${other.nodeName}; not a known passthrough")
      None
  }

}
