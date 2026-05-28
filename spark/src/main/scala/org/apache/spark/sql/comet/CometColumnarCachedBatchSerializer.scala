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

package org.apache.spark.sql.comet

import java.io.Serializable
import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometConf

/**
 * Comet's implementation of Spark's CachedBatchSerializer.
 *
 * Stores cached data as Arrow IPC bytes (same format Comet already uses for shuffle and
 * broadcast). When InMemoryTableScan reads cached data it produces ColumnarBatch directly,
 * so the Comet native chain is never broken at an AQE cache boundary.
 *
 * Enable via:
 *   spark.sql.cache.serializer = org.apache.spark.sql.comet.CometColumnarCachedBatchSerializer
 *
 * Falls back to Spark's DefaultCachedBatchSerializer for schemas that Comet cannot handle
 * (e.g. complex nested types not supported by CometArrowConverters).
 */
class CometColumnarCachedBatchSerializer extends CachedBatchSerializer with Serializable {

  private lazy val fallback = new DefaultCachedBatchSerializer

  private def toStructType(schema: Seq[Attribute]): StructType =
    StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  private def isSupportedSchema(schema: Seq[Attribute]): Boolean =
    !schema.exists(a => containsMap(a.dataType))

  private def isSupportedSchema(schema: StructType): Boolean =
    !schema.fields.exists(f => containsMap(f.dataType))

  private def containsMap(dt: DataType): Boolean = dt match {
    case _: MapType => true
    case ArrayType(elementType, _) => containsMap(elementType)
    case StructType(fields) => fields.exists(f => containsMap(f.dataType))
    case _ => false
  }

  /**
   * Whether this serializer supports columnar input. When true, Spark calls
   * convertColumnarBatchToCachedBatch instead of convertInternalRowToCachedBatch.
   */
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean =
    isSupportedSchema(schema)

  /**
   * Whether this serializer produces ColumnarBatch on read. When true,
   * InMemoryTableScan calls convertCachedBatchToColumnarBatch and returns Arrow
   * ColumnarBatch directly with no row-to-columnar conversion overhead.
   */
  override def supportsColumnarOutput(schema: StructType): Boolean =
    isSupportedSchema(schema)

  /**
   * Serialize Arrow ColumnarBatches into CachedBatch (Arrow IPC bytes).
   * Called when caching columnar data from Comet operators.
   */
  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    if (!isSupportedSchema(schema)) {
      return fallback.convertColumnarBatchToCachedBatch(input, schema, storageLevel, conf)
    }
    input.mapPartitions { batches =>
      Utils.serializeBatches(batches).map { case (numRows, bytes) =>
        ArrowCachedBatch(numRows.toInt, bytes.toArray)
      }
    }
  }

  /**
   * Serialize InternalRows into CachedBatch (Arrow IPC bytes).
   * Called when caching row-based data (e.g. from non-Comet operators).
   */
  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    if (!isSupportedSchema(schema)) {
      return fallback.convertInternalRowToCachedBatch(input, schema, storageLevel, conf)
    }
    val batchSize = CometConf.COMET_BATCH_SIZE.get(conf)
    val structSchema = toStructType(schema)
    input.mapPartitions { rows =>
      val arrowIter = CometArrowConverters.rowToArrowBatchIter(
        rows, structSchema, batchSize, "UTC", null)
      Utils.serializeBatches(arrowIter).map { case (numRows, bytes) =>
        ArrowCachedBatch(numRows.toInt, bytes.toArray)
      }
    }
  }

  /**
   * Deserialize CachedBatch to ColumnarBatch (Arrow IPC bytes to Arrow ColumnarBatch).
   * Called by InMemoryTableScan when supportsColumnarOutput = true.
   * Zero-conversion path: Arrow bytes go directly into Comet native execution.
   *
   * Supports column pruning: only selectedAttributes columns are materialized.
   */
  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (!isSupportedSchema(cacheAttributes)) {
      return fallback.convertCachedBatchToColumnarBatch(
        input, cacheAttributes, selectedAttributes, conf)
    }

    val requestedIndices = selectedAttributes.map { a =>
      cacheAttributes.indexWhere(_.exprId == a.exprId)
    }
    val needsPruning = cacheAttributes != selectedAttributes

    input.mapPartitions { batches =>
      batches.flatMap {
        case ArrowCachedBatch(_, bytes) =>
          val buf = new ChunkedByteBuffer(Array(ByteBuffer.wrap(bytes)))
          val allBatches = Utils.decodeBatches(buf, "CometColumnarCachedBatchSerializer")
          if (needsPruning) {
            allBatches.map { batch =>
              val cols = requestedIndices.map(i => batch.column(i)).toArray
              new ColumnarBatch(
                cols.asInstanceOf[Array[ColumnVector]],
                batch.numRows())
            }
          } else {
            allBatches
          }
        case other =>
          throw new IllegalArgumentException(
            "CometColumnarCachedBatchSerializer: unexpected CachedBatch type: " +
              other.getClass.getName)
      }
    }
  }

  /**
   * Deserialize CachedBatch to InternalRow.
   * Fallback path when columnar output is not requested (e.g. non-Comet consumer).
   * Re-serializes as UnsafeRow via the default Spark serializer.
   */
  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (!isSupportedSchema(cacheAttributes)) {
      return fallback.convertCachedBatchToInternalRow(
        input, cacheAttributes, selectedAttributes, conf)
    }
    val columnar = convertCachedBatchToColumnarBatch(
      input, cacheAttributes, selectedAttributes, conf)
    val reEncoded = fallback.convertColumnarBatchToCachedBatch(
      columnar, selectedAttributes, StorageLevel.MEMORY_ONLY, conf)
    fallback.convertCachedBatchToInternalRow(
      reEncoded, selectedAttributes, selectedAttributes, conf)
  }

  /**
   * Filter predicate for pre-filtering cached batches before decompression.
   * Arrow IPC does not embed per-column statistics so all batches pass through.
   */
  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] =
    (_, batches) => batches
}

/**
 * A CachedBatch wrapping Arrow IPC serialized bytes.
 * Produced by CometColumnarCachedBatchSerializer and consumed by InMemoryTableScan
 * to produce ColumnarBatch directly without row conversion.
 */
case class ArrowCachedBatch(override val numRows: Int, bytes: Array[Byte])
    extends CachedBatch {
  override def sizeInBytes: Long = bytes.length.toLong
}
