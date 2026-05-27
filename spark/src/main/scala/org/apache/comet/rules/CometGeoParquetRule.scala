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

import java.util.concurrent.ConcurrentHashMap

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.expressions.StGeomFromWkb
import org.apache.comet.parquet.{GeoParquetFunctions, GeoParquetMetadata}

/**
 * Optimizer rule that detects GeoParquet files and automatically wraps WKB-encoded geometry
 * columns with StGeomFromWkb so downstream geo functions work without manual casting.
 *
 * Fires only when the Parquet footer contains a "geo" key listing WKB-encoded columns. Strict
 * no-op for all non-GeoParquet files. Footer reads are cached by file path so repeated optimizer
 * invocations on the same relation pay the IO cost only once.
 */
case class CometGeoParquetRule(session: SparkSession) extends Rule[LogicalPlan] {

  // Register st_geoparquet_metadata / st_geoparquet_columns as Spark SQL UDFs.
  // injectOptimizerRule creates this rule once per session so this runs exactly once.
  GeoParquetFunctions.registerAll(session)

  // Cache: path string -> set of WKB column names (empty = not a GeoParquet file).
  // Avoids reading the Parquet footer on every optimizer invocation for the same file.
  private val footerCache: ConcurrentHashMap[String, Set[String]] =
    new ConcurrentHashMap[String, Set[String]]()

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case lr @ LogicalRelation(r: HadoopFsRelation, output, _, _)
        if r.fileFormat.isInstanceOf[ParquetFileFormat] =>
      val wkbCols = detectWkbColumns(r)
      if (wkbCols.isEmpty) {
        lr
      } else {
        val newProjectList: Seq[NamedExpression] = output.map { attr =>
          if (wkbCols.contains(attr.name.toLowerCase(java.util.Locale.ROOT)) &&
              attr.dataType == BinaryType) {
            // Alias keeps the original column name so downstream SQL is unaffected.
            Alias(StGeomFromWkb(attr), attr.name)(attr.exprId, attr.qualifier)
          } else {
            attr
          }
        }
        if (newProjectList == output) lr
        else Project(newProjectList, lr)
      }
  }

  private def detectWkbColumns(r: HadoopFsRelation): Set[String] = {
    Try {
      val hadoopConf: Configuration =
        r.sparkSession.sessionState.newHadoopConfWithOptions(r.options)

      val firstPath: Option[Path] = Try {
        r.location.listFiles(Seq.empty, Seq.empty).flatMap(_.files).headOption.map { f =>
          new Path(f.getPath.toString)
        }
      }.getOrElse(None)

      firstPath match {
        case None => Set.empty[String]
        case Some(path) =>
          val key = path.toString
          // computeIfAbsent is atomic -- footer is read at most once per path per JVM.
          footerCache.computeIfAbsent(
            key,
            _ =>
              GeoParquetMetadata
                .read(path, hadoopConf)
                .map(_.wkbColumnNames)
                .getOrElse(Set.empty[String]))
      }
    }.getOrElse(Set.empty[String])
  }
}
