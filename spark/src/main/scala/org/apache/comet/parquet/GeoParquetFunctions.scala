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

package org.apache.comet.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * Utilities for reading GeoParquet file metadata from user code.
 *
 * Usage from Python/Scala:
 * {{{
 *   // Register helper UDFs on the session
 *   GeoParquetFunctions.registerAll(spark)
 *
 *   // Then from SQL or DataFrame API:
 *   spark.sql("SELECT st_geoparquet_metadata('/path/to/file.parquet')").show(false)
 *   spark.sql("SELECT st_geoparquet_columns('/path/to/file.parquet')").show(false)
 * }}}
 *
 * Or call directly from Scala without registering:
 * {{{
 *   val meta = GeoParquetFunctions.readMetadata(spark, "/path/to/file.parquet")
 *   meta.foreach { m =>
 *     println(s"version: ${m.version}, primary: ${m.primary}")
 *     m.columns.foreach { case (name, info) =>
 *       println(s"  $name: encoding=${info.encoding} bbox=${info.bbox}")
 *     }
 *   }
 * }}}
 */
object GeoParquetFunctions {

  /**
   * Returns the parsed GeoParquetMetadata for a file path, or None if the file is not a
   * GeoParquet file or on any error.
   */
  def readMetadata(spark: SparkSession, filePath: String): Option[GeoParquetMetadata] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    GeoParquetMetadata.read(new Path(filePath), hadoopConf)
  }

  /**
   * Returns the raw "geo" JSON string from the Parquet file footer, or null if absent. Useful for
   * inspecting the full GeoParquet metadata blob without parsing.
   */
  def readRawGeoJson(spark: SparkSession, filePath: String): Option[String] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    GeoParquetMetadata.readRawJson(new Path(filePath), hadoopConf)
  }

  /**
   * Registers two Spark SQL UDFs on the given session:
   *
   * st_geoparquet_metadata(path STRING) -> STRING Returns the raw "geo" JSON string from the
   * Parquet file footer at `path`, or NULL if not a GeoParquet file. The JSON contains version,
   * primary_column, and per-column info (encoding, geometry_types, bbox, crs).
   *
   * st_geoparquet_columns(path STRING) -> ARRAY Returns the sorted list of WKB geometry column
   * names in the file, or an empty array if none / not a GeoParquet file.
   */
  def registerAll(spark: SparkSession): Unit = {
    // Capture hadoopConf on the driver; SparkSession cannot be used inside UDF closures
    // because it is not serializable and its sessionState is null on executors.
    val hadoopConf: Configuration = spark.sessionState.newHadoopConf()

    spark.udf.register(
      "st_geoparquet_metadata",
      (path: String) => GeoParquetMetadata.readRawJson(new Path(path), hadoopConf).orNull)

    spark.udf.register(
      "st_geoparquet_columns",
      (path: String) =>
        GeoParquetMetadata
          .read(new Path(path), hadoopConf)
          .map(_.wkbColumnNames.toArray.sorted)
          .getOrElse(Array.empty[String]))
  }
}
