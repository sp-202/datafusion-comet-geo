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

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Parsed representation of one geometry column from GeoParquet "geo" metadata.
 *
 * @param encoding       always "WKB" for files we handle natively
 * @param geometryTypes  list of geometry type strings, e.g. ["Polygon", "MultiPolygon"]
 * @param bbox           optional bounding box [xmin, ymin, xmax, ymax]
 * @param crs            optional CRS as raw JSON string (PROJJSON object)
 */
case class GeoColumnInfo(
    encoding: String,
    geometryTypes: Seq[String],
    bbox: Option[(Double, Double, Double, Double)],
    crs: Option[String])

/**
 * Parsed "geo" metadata block from a GeoParquet file footer.
 *
 * @param version  GeoParquet spec version string, e.g. "1.1.0"
 * @param primary  name of the primary geometry column
 * @param columns  map from column name (lowercase) to its GeoColumnInfo
 */
case class GeoParquetMetadata(
    version: String,
    primary: String,
    columns: Map[String, GeoColumnInfo]) {

  /** Returns the names of all WKB-encoded geometry columns (lowercase). */
  def wkbColumnNames: Set[String] =
    columns.collect {
      case (name, info) if info.encoding.toUpperCase(java.util.Locale.ROOT) == "WKB" => name
    }.toSet
}

object GeoParquetMetadata {

  implicit val formats: DefaultFormats.type = DefaultFormats

  /**
   * Reads the Parquet footer of the given file path and parses the "geo" metadata key.
   * Returns None if the file is not a GeoParquet file or on any IO / parse error.
   */
  def read(path: Path, hadoopConf: Configuration): Option[GeoParquetMetadata] = {
    Try {
      val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
      val reader = ParquetFileReader.open(inputFile)
      val rawJson: Option[String] =
        try {
          Option(reader.getFileMetaData.getKeyValueMetaData.get("geo"))
        } finally {
          reader.close()
        }
      rawJson.flatMap(parseGeoJson)
    }.getOrElse(None)
  }

  /**
   * Returns the raw "geo" JSON string from the given Parquet file footer, or None if absent or
   * on any error. Used by st_geoparquet_metadata() to expose the full blob to users.
   */
  def readRawJson(path: Path, hadoopConf: Configuration): Option[String] = {
    Try {
      val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
      val reader = ParquetFileReader.open(inputFile)
      try {
        Option(reader.getFileMetaData.getKeyValueMetaData.get("geo"))
      } finally {
        reader.close()
      }
    }.getOrElse(None)
  }

  /**
   * Parses the raw GeoParquet "geo" JSON string into a GeoParquetMetadata. Returns None on any
   * parse failure.
   *
   * GeoParquet spec v1.1.0 shape:
   * {{{
   *   {
   *     "version": "1.1.0",
   *     "primary_column": "geometry",
   *     "columns": {
   *       "geometry": {
   *         "encoding": "WKB",
   *         "geometry_types": ["Polygon"],
   *         "bbox": [xmin, ymin, xmax, ymax],
   *         "crs": { ... PROJJSON ... }
   *       }
   *     }
   *   }
   * }}}
   */
  def parseGeoJson(json: String): Option[GeoParquetMetadata] = {
    Try {
      val root = parse(json)
      val version = (root \ "version").extractOpt[String].getOrElse("unknown")
      val primary = (root \ "primary_column").extractOpt[String].getOrElse("")

      val columnsNode = root \ "columns"
      val columns: Map[String, GeoColumnInfo] = columnsNode match {
        case JObject(fields) =>
          fields.map { case (colName, colMeta) =>
            val encoding = (colMeta \ "encoding").extractOpt[String].getOrElse("")
            val geomTypes =
              (colMeta \ "geometry_types").extractOpt[List[String]].getOrElse(Nil)
            val bbox = (colMeta \ "bbox").extractOpt[List[Double]].flatMap {
              case List(xmin, ymin, xmax, ymax) => Some((xmin, ymin, xmax, ymax))
              case _ => None
            }
            val crs = (colMeta \ "crs") match {
              case JNothing | JNull => None
              case node => Some(compact(render(node)))
            }
            val lowerName = colName.toLowerCase(java.util.Locale.ROOT)
            lowerName -> GeoColumnInfo(encoding, geomTypes, bbox, crs)
          }.toMap
        case _ => Map.empty
      }

      GeoParquetMetadata(version, primary, columns)
    }.toOption
  }
}
