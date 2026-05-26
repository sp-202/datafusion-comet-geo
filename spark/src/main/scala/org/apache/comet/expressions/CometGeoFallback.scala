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

package org.apache.comet.expressions

/**
 * JVM fallback implementations for Comet geo UDFs. Called only when Comet native execution is
 * disabled. When Comet is active the expression is serded to ScalarFunc and executed via the Rust
 * geo crate in DataFusion.
 *
 * Requires Apache Sedona on the classpath to function. Without Sedona and without Comet enabled
 * an UnsupportedOperationException is thrown at runtime.
 */
object CometGeoFallback {

  private def notSupported(fn: String): Nothing =
    throw new UnsupportedOperationException(
      s"$fn requires either Comet native execution (spark.comet.exec.enabled=true) " +
        s"or Apache Sedona on the classpath for JVM fallback.")

  // Constructors - return WKB bytes
  def geomFromWkt(g: String): Array[Byte] = notSupported("st_geomfromwkt")
  def geomFromGeoJson(g: String): Array[Byte] = notSupported("st_geomfromgeojson")
  def makeEnvelope(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Array[Byte] =
    notSupported("st_makeenvelope")
  def makePoint(x: Double, y: Double): Array[Byte] = notSupported("st_point")
  def makeLine(g1: Array[Byte], g2: Array[Byte]): Array[Byte] = notSupported("st_makeline")
  // Serializers - asText returns String (WKT), asGeoJson returns String (JSON)
  def asText(g: Array[Byte]): String = notSupported("st_astext")
  def asGeoJson(g: Array[Byte]): String = notSupported("st_asgeojson")
  // Predicates - take WKB bytes
  def contains(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_contains")
  def intersects(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_intersects")
  def within(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_within")
  def covers(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_covers")
  def coveredBy(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_coveredby")
  def equals(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_equals")
  def touches(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_touches")
  def crosses(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_crosses")
  def disjoint(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_disjoint")
  def overlaps(g1: Array[Byte], g2: Array[Byte]): Boolean = notSupported("st_overlaps")
  // Measurements
  def distance(g1: Array[Byte], g2: Array[Byte]): Double = notSupported("st_distance")
  def distanceSphere(g1: Array[Byte], g2: Array[Byte]): Double = notSupported("st_distancesphere")
  def area(g: Array[Byte]): Double = notSupported("st_area")
  def length(g: Array[Byte]): Double = notSupported("st_length")
  def perimeter(g: Array[Byte]): Double = notSupported("st_perimeter")
  def hausdorffDistance(g1: Array[Byte], g2: Array[Byte]): Double =
    notSupported("st_hausdorffdistance")
  // Transformations - return WKB bytes
  def centroid(g: Array[Byte]): Array[Byte] = notSupported("st_centroid")
  def envelope(g: Array[Byte]): Array[Byte] = notSupported("st_envelope")
  def convexHull(g: Array[Byte]): Array[Byte] = notSupported("st_convexhull")
  def simplify(g: Array[Byte], tolerance: Double): Array[Byte] = notSupported("st_simplify")
  def simplifyPreserveTopology(g: Array[Byte], tolerance: Double): Array[Byte] =
    notSupported("st_simplifypreservetopology")
  def flipCoordinates(g: Array[Byte]): Array[Byte] = notSupported("st_flipcoordinates")
  def boundary(g: Array[Byte]): Array[Byte] = notSupported("st_boundary")
  def buffer(g: Array[Byte], distance: Double): Array[Byte] = notSupported("st_buffer")
  // Set operations - return WKB bytes
  def union(g1: Array[Byte], g2: Array[Byte]): Array[Byte] = notSupported("st_union")
  def intersection(g1: Array[Byte], g2: Array[Byte]): Array[Byte] =
    notSupported("st_intersection")
  def difference(g1: Array[Byte], g2: Array[Byte]): Array[Byte] = notSupported("st_difference")
  def symDifference(g1: Array[Byte], g2: Array[Byte]): Array[Byte] =
    notSupported("st_symdifference")
  // Accessors
  def isEmpty(g: Array[Byte]): Boolean = notSupported("st_isempty")
  def geometryType(g: Array[Byte]): String = notSupported("st_geometrytype")
  def numPoints(g: Array[Byte]): Long = notSupported("st_numpoints")
  def stX(g: Array[Byte]): Double = notSupported("st_x")
  def stY(g: Array[Byte]): Double = notSupported("st_y")
}
