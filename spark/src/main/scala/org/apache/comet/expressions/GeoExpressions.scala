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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

// Marker trait - lets the optimizer rule identify all geo expressions without
// pattern-matching every case class individually.
// CodegenFallback makes Spark use interpreted eval when a geo expression ends up
// inside a JVM WholeStageCodegen context (e.g. result projection of a JVM aggregate).
trait CometGeoExpression extends CodegenFallback

// ---- Binary geo predicates -----------------------------------------------

case class StContains(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.contains(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StIntersects(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.intersects(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StWithin(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.within(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StDistance(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.distance(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StUnion(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.union(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StIntersection(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.intersection(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// StPoint takes two Double arguments (x, y) and returns a WKB geometry
case class StPoint(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(x: Any, y: Any): Any =
    CometGeoFallback.makePoint(x.asInstanceOf[Double], y.asInstanceOf[Double])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// ---- Unary geo functions --------------------------------------------------

case class StArea(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.area(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StCentroid(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.centroid(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StLength(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.length(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StIsEmpty(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.isEmpty(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StGeometryType(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.geometryType(g.asInstanceOf[Array[Byte]]))
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StNumPoints(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = LongType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.numPoints(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StX(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.stX(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StY(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.stY(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StEnvelope(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.envelope(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StConvexHull(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.convexHull(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// st_simplify and st_buffer take two args (geom + numeric param)

case class StSimplify(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any, t: Any): Any =
    CometGeoFallback.simplify(g.asInstanceOf[Array[Byte]], t.asInstanceOf[Double])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StBuffer(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any, d: Any): Any =
    CometGeoFallback.buffer(g.asInstanceOf[Array[Byte]], d.asInstanceOf[Double])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// ---- Constructors --------------------------------------------------------

// StGeomFromWkb takes Binary (WKB) and returns Binary (WKB) (validates and normalises)
case class StGeomFromWkb(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.geomFromWkb(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// StAsBinary takes Binary (WKB geometry) and returns Binary (raw WKB bytes)
case class StAsBinary(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any = g.asInstanceOf[Array[Byte]]
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// StGeomFromWkt takes a String (WKT) and returns Binary (WKB)
case class StGeomFromWkt(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.geomFromWkt(g.asInstanceOf[UTF8String].toString)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// StGeomFromGeoJson takes a String (JSON) and returns Binary (WKB)
case class StGeomFromGeoJson(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.geomFromGeoJson(g.asInstanceOf[UTF8String].toString)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StMakeEnvelope(xmin: Expression, ymin: Expression, xmax: Expression, ymax: Expression)
    extends Expression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(xmin, ymin, xmax, ymax)
  override def eval(input: InternalRow): Any = {
    val xv = xmin.eval(input)
    val yv = ymin.eval(input)
    val xv2 = xmax.eval(input)
    val yv2 = ymax.eval(input)
    if (xv == null || yv == null || xv2 == null || yv2 == null) {
      null
    } else {
      CometGeoFallback.makeEnvelope(
        xv.asInstanceOf[Double],
        yv.asInstanceOf[Double],
        xv2.asInstanceOf[Double],
        yv2.asInstanceOf[Double])
    }
  }
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(
      xmin = newChildren(0),
      ymin = newChildren(1),
      xmax = newChildren(2),
      ymax = newChildren(3))
}

case class StMakeLine(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.makeLine(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// ---- Serializers ---------------------------------------------------------

// StAsText takes Binary (WKB) and returns String (WKT)
case class StAsText(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.asText(g.asInstanceOf[Array[Byte]]))
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// StAsGeoJson takes Binary (WKB) and returns String (JSON)
case class StAsGeoJson(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = StringType
  override def nullSafeEval(g: Any): Any =
    UTF8String.fromString(CometGeoFallback.asGeoJson(g.asInstanceOf[Array[Byte]]))
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// ---- Additional predicates -----------------------------------------------

case class StCovers(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.covers(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StCoveredBy(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.coveredBy(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StEquals(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.equals(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StTouches(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.touches(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StCrosses(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.crosses(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StDisjoint(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.disjoint(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StOverlaps(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.overlaps(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// ---- Additional measurements ---------------------------------------------

case class StDistanceSphere(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.distanceSphere(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StPerimeter(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.perimeter(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StHausdorffDistance(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = DoubleType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.hausdorffDistance(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// ---- Additional transformations ------------------------------------------

case class StSimplifyPreserveTopology(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.simplifyPreserveTopology(
      g1.asInstanceOf[Array[Byte]],
      g2.asInstanceOf[Double])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StFlipCoordinates(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.flipCoordinates(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StBoundary(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.boundary(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// ---- New geo-parquet accessors and transformations -----------------------

case class StIsValid(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BooleanType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.isValid(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StSrid(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = IntegerType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.srid(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StDimension(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = IntegerType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.dimension(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StNumGeometries(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = IntegerType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.numGeometries(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StStartPoint(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.startPoint(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StEndPoint(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.endPoint(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StExteriorRing(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.exteriorRing(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StNumInteriorRings(child: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = IntegerType
  override def nullSafeEval(g: Any): Any =
    CometGeoFallback.numInteriorRings(g.asInstanceOf[Array[Byte]])
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class StTranslate(geom: Expression, deltaX: Expression, deltaY: Expression)
    extends Expression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(geom, deltaX, deltaY)
  override def eval(input: InternalRow): Any = {
    val g = geom.eval(input)
    val dx = deltaX.eval(input)
    val dy = deltaY.eval(input)
    if (g == null || dx == null || dy == null) {
      null
    } else {
      CometGeoFallback.translate(
        g.asInstanceOf[Array[Byte]],
        dx.asInstanceOf[Double],
        dy.asInstanceOf[Double])
    }
  }
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(geom = newChildren(0), deltaX = newChildren(1), deltaY = newChildren(2))
}

// ---- Additional set operations -------------------------------------------

case class StDifference(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.difference(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

case class StSymDifference(left: Expression, right: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CometGeoExpression {
  override def foldable: Boolean = false
  override def dataType: DataType = BinaryType
  override def nullSafeEval(g1: Any, g2: Any): Any =
    CometGeoFallback.symDifference(g1.asInstanceOf[Array[Byte]], g2.asInstanceOf[Array[Byte]])
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

// ---- Registration helpers for SparkSessionExtensions.injectFunction ------

object GeoExpressions {

  type FunctionDescription =
    (FunctionIdentifier, ExpressionInfo, Seq[Expression] => Expression)

  private def desc(
      name: String,
      cls: Class[_],
      builder: Seq[Expression] => Expression): FunctionDescription =
    (new FunctionIdentifier(name), new ExpressionInfo(cls.getName, name), builder)

  val stContainsInfo: FunctionDescription =
    desc("st_contains", classOf[StContains], { args => StContains(args(0), args(1)) })

  val stIntersectsInfo: FunctionDescription =
    desc("st_intersects", classOf[StIntersects], { args => StIntersects(args(0), args(1)) })

  val stWithinInfo: FunctionDescription =
    desc("st_within", classOf[StWithin], { args => StWithin(args(0), args(1)) })

  val stDistanceInfo: FunctionDescription =
    desc("st_distance", classOf[StDistance], { args => StDistance(args(0), args(1)) })

  val stAreaInfo: FunctionDescription =
    desc("st_area", classOf[StArea], { args => StArea(args(0)) })

  val stCentroidInfo: FunctionDescription =
    desc("st_centroid", classOf[StCentroid], { args => StCentroid(args(0)) })

  val stLengthInfo: FunctionDescription =
    desc("st_length", classOf[StLength], { args => StLength(args(0)) })

  val stIsEmptyInfo: FunctionDescription =
    desc("st_isempty", classOf[StIsEmpty], { args => StIsEmpty(args(0)) })

  val stGeometryTypeInfo: FunctionDescription =
    desc("st_geometrytype", classOf[StGeometryType], { args => StGeometryType(args(0)) })

  val stNumPointsInfo: FunctionDescription =
    desc("st_numpoints", classOf[StNumPoints], { args => StNumPoints(args(0)) })

  val stXInfo: FunctionDescription =
    desc("st_x", classOf[StX], { args => StX(args(0)) })

  val stYInfo: FunctionDescription =
    desc("st_y", classOf[StY], { args => StY(args(0)) })

  val stEnvelopeInfo: FunctionDescription =
    desc("st_envelope", classOf[StEnvelope], { args => StEnvelope(args(0)) })

  val stConvexHullInfo: FunctionDescription =
    desc("st_convexhull", classOf[StConvexHull], { args => StConvexHull(args(0)) })

  val stSimplifyInfo: FunctionDescription =
    desc("st_simplify", classOf[StSimplify], { args => StSimplify(args(0), args(1)) })

  val stBufferInfo: FunctionDescription =
    desc("st_buffer", classOf[StBuffer], { args => StBuffer(args(0), args(1)) })

  val stUnionInfo: FunctionDescription =
    desc("st_union", classOf[StUnion], { args => StUnion(args(0), args(1)) })

  val stIntersectionInfo: FunctionDescription =
    desc("st_intersection", classOf[StIntersection], { args => StIntersection(args(0), args(1)) })

  val stGeomFromWktInfo: FunctionDescription =
    desc("st_geomfromwkt", classOf[StGeomFromWkt], { args => StGeomFromWkt(args(0)) })

  val stGeomFromGeoJsonInfo: FunctionDescription =
    desc("st_geomfromgeojson", classOf[StGeomFromGeoJson], { args => StGeomFromGeoJson(args(0)) })

  val stPointInfo: FunctionDescription =
    desc("st_point", classOf[StPoint], { args => StPoint(args(0), args(1)) })

  val stMakeEnvelopeInfo: FunctionDescription =
    desc(
      "st_makeenvelope",
      classOf[StMakeEnvelope],
      { args => StMakeEnvelope(args(0), args(1), args(2), args(3)) })

  val stMakeLineInfo: FunctionDescription =
    desc("st_makeline", classOf[StMakeLine], { args => StMakeLine(args(0), args(1)) })

  val stAsTextInfo: FunctionDescription =
    desc("st_astext", classOf[StAsText], { args => StAsText(args(0)) })

  val stAsGeoJsonInfo: FunctionDescription =
    desc("st_asgeojson", classOf[StAsGeoJson], { args => StAsGeoJson(args(0)) })

  val stCoversInfo: FunctionDescription =
    desc("st_covers", classOf[StCovers], { args => StCovers(args(0), args(1)) })

  val stCoveredByInfo: FunctionDescription =
    desc("st_coveredby", classOf[StCoveredBy], { args => StCoveredBy(args(0), args(1)) })

  val stEqualsInfo: FunctionDescription =
    desc("st_equals", classOf[StEquals], { args => StEquals(args(0), args(1)) })

  val stTouchesInfo: FunctionDescription =
    desc("st_touches", classOf[StTouches], { args => StTouches(args(0), args(1)) })

  val stCrossesInfo: FunctionDescription =
    desc("st_crosses", classOf[StCrosses], { args => StCrosses(args(0), args(1)) })

  val stDisjointInfo: FunctionDescription =
    desc("st_disjoint", classOf[StDisjoint], { args => StDisjoint(args(0), args(1)) })

  val stOverlapsInfo: FunctionDescription =
    desc("st_overlaps", classOf[StOverlaps], { args => StOverlaps(args(0), args(1)) })

  val stDistanceSphereInfo: FunctionDescription =
    desc(
      "st_distancesphere",
      classOf[StDistanceSphere],
      { args => StDistanceSphere(args(0), args(1)) })

  val stPerimeterInfo: FunctionDescription =
    desc("st_perimeter", classOf[StPerimeter], { args => StPerimeter(args(0)) })

  val stHausdorffDistanceInfo: FunctionDescription =
    desc(
      "st_hausdorffdistance",
      classOf[StHausdorffDistance],
      { args => StHausdorffDistance(args(0), args(1)) })

  val stSimplifyPreserveTopologyInfo: FunctionDescription =
    desc(
      "st_simplifypreservetopology",
      classOf[StSimplifyPreserveTopology],
      { args => StSimplifyPreserveTopology(args(0), args(1)) })

  val stFlipCoordinatesInfo: FunctionDescription =
    desc("st_flipcoordinates", classOf[StFlipCoordinates], { args => StFlipCoordinates(args(0)) })

  val stBoundaryInfo: FunctionDescription =
    desc("st_boundary", classOf[StBoundary], { args => StBoundary(args(0)) })

  val stDifferenceInfo: FunctionDescription =
    desc("st_difference", classOf[StDifference], { args => StDifference(args(0), args(1)) })

  val stSymDifferenceInfo: FunctionDescription =
    desc(
      "st_symdifference",
      classOf[StSymDifference],
      { args => StSymDifference(args(0), args(1)) })

  val stGeomFromWkbInfo: FunctionDescription =
    desc("st_geomfromwkb", classOf[StGeomFromWkb], { args => StGeomFromWkb(args(0)) })

  val stAsBinaryInfo: FunctionDescription =
    desc("st_asbinary", classOf[StAsBinary], { args => StAsBinary(args(0)) })

  val stIsValidInfo: FunctionDescription =
    desc("st_isvalid", classOf[StIsValid], { args => StIsValid(args(0)) })

  val stSridInfo: FunctionDescription =
    desc("st_srid", classOf[StSrid], { args => StSrid(args(0)) })

  val stDimensionInfo: FunctionDescription =
    desc("st_dimension", classOf[StDimension], { args => StDimension(args(0)) })

  val stNumGeometriesInfo: FunctionDescription =
    desc("st_numgeometries", classOf[StNumGeometries], { args => StNumGeometries(args(0)) })

  val stStartPointInfo: FunctionDescription =
    desc("st_startpoint", classOf[StStartPoint], { args => StStartPoint(args(0)) })

  val stEndPointInfo: FunctionDescription =
    desc("st_endpoint", classOf[StEndPoint], { args => StEndPoint(args(0)) })

  val stExteriorRingInfo: FunctionDescription =
    desc("st_exteriorring", classOf[StExteriorRing], { args => StExteriorRing(args(0)) })

  val stNumInteriorRingsInfo: FunctionDescription =
    desc(
      "st_numinteriorrings",
      classOf[StNumInteriorRings],
      { args => StNumInteriorRings(args(0)) })

  val stTranslateInfo: FunctionDescription =
    desc("st_translate", classOf[StTranslate], { args => StTranslate(args(0), args(1), args(2)) })
}
