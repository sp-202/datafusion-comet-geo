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

import scala.util.Try

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.expressions.StGeomFromWkb

/**
 * Optimizer rule that detects GeoParquet files and automatically wraps WKB-encoded geometry
 * columns with StGeomFromWkb so downstream geo functions work without manual casting.
 *
 * The rule fires only when ALL of the following are true:
 *   1. The scan is a Parquet file (HadoopFsRelation with ParquetFileFormat)
 *   2. The Parquet file footer contains a "geo" key in its key-value metadata
 *   3. The "geo" JSON lists at least one column with "encoding": "WKB"
 *   4. That column exists in the scan's output with BinaryType
 *
 * Any failure (IO error, malformed JSON, missing column) is caught and the plan is returned
 * unchanged. This rule is a strict no-op for all non-GeoParquet files.
 */
case class CometGeoParquetRule(session: SparkSession) extends Rule[LogicalPlan] {

  implicit val formats: DefaultFormats.type = DefaultFormats

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case lr @ LogicalRelation(r: HadoopFsRelation, output, _, _)
        if r.fileFormat.isInstanceOf[ParquetFileFormat] =>
      val wkbCols = detectWkbColumns(r)
      if (wkbCols.isEmpty) {
        lr
      } else {
        // Build a Project that wraps each geometry column with StGeomFromWkb.
        // Non-geometry columns pass through as-is.
        val newProjectList: Seq[NamedExpression] = output.map { attr =>
          if (wkbCols.contains(attr.name.toLowerCase) && attr.dataType == BinaryType) {
            // Alias preserves the original column name so downstream SQL is unaffected.
            Alias(StGeomFromWkb(attr), attr.name)(attr.exprId, attr.qualifier)
          } else {
            attr
          }
        }
        // Only insert the Project if we actually changed something.
        if (newProjectList == output) lr
        else Project(newProjectList, lr)
      }
  }

  /**
   * Reads the Parquet footer of the first available file in the relation and extracts the set of
   * column names whose GeoParquet encoding is "WKB". Returns empty set on any error.
   */
  private def detectWkbColumns(r: HadoopFsRelation): Set[String] = {
    Try {
      val hadoopConf: Configuration =
        r.sparkSession.sessionState.newHadoopConfWithOptions(r.options)

      // Pick the first file from the relation. All files in a partition share the same schema
      // and geo metadata.
      val firstFile: Option[Path] = Try {
        r.location.listFiles(Seq.empty, Seq.empty).flatMap(_.files).headOption.map { f =>
          new Path(f.getPath.toString)
        }
      }.getOrElse(None)

      firstFile match {
        case None => Set.empty[String]
        case Some(path) =>
          val geoJson: Option[String] = Try {
            val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
            val reader = ParquetFileReader.open(inputFile)
            try {
              val meta = reader.getFileMetaData.getKeyValueMetaData
              Option(meta.get("geo"))
            } finally {
              reader.close()
            }
          }.getOrElse(None)

          geoJson match {
            case None => Set.empty[String]
            case Some(json) => parseWkbColumns(json)
          }
      }
    }.getOrElse(Set.empty[String])
  }

  /**
   * Parses the GeoParquet "geo" JSON blob and returns the lowercase names of all columns whose
   * encoding is "WKB" (case-insensitive). Returns empty set on any parse error.
   *
   * GeoParquet spec v1.1.0 "geo" metadata shape:
   * {{{
   *   {
   *     "version": "1.1.0",
   *     "columns": {
   *       "geometry": { "encoding": "WKB", ... },
   *       "other_col": { "encoding": "WKB", ... }
   *     }
   *   }
   * }}}
   */
  private def parseWkbColumns(json: String): Set[String] = {
    Try {
      val root = parse(json)
      val columns = (root \ "columns").asInstanceOf[JObject]
      columns.obj.collect {
        case (colName, colMeta) =>
          val encoding = (colMeta \ "encoding").extractOpt[String].getOrElse("")
          (colName.toLowerCase, encoding.toUpperCase)
      }.collect {
        case (colName, "WKB") => colName
      }.toSet
    }.getOrElse(Set.empty[String])
  }
}
