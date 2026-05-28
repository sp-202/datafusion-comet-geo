<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Comet Geo Feature Implementation Plan

Branch: `geo-parquet` (47 commits ahead of main as of 2026-05-27)

---

## What Is Already Built

| Feature | Status | Key Files |
|---|---|---|
| 53 ST_ geo functions (native Rust) | Done | `expressions/geo/*.rs`, `GeoExpressions.scala`, `serde/geo.scala` |
| GeoParquet auto-detection (read) | Done | `CometGeoParquetRule.scala`, `GeoParquetMetadata.scala` |
| `st_geoparquet_metadata` / `st_geoparquet_columns` UDFs | Done | `GeoParquetFunctions.scala` |
| Native window functions (rank, offset, AVG) | Done | `CometWindowExec`, window Rust ops |
| `CometGeoPreAggregateRule` | Done | `CometGeoPreAggregateRule.scala` |

---

## Phase 1 — Write GeoParquet

**Goal:** When writing a DataFrame with WKB geometry columns to Parquet, automatically inject the OGC GeoParquet `geo` JSON key in the Parquet file footer so the output is a valid GeoParquet file.

**Why it matters:** Completes the read/write cycle. Users can load a shapefile, process it with ST_ functions, and write it back as GeoParquet that any GeoParquet-compatible tool (QGIS, GeoPandas, DuckDB) can read.

**Approach:** Pure Scala — no Rust or protobuf changes needed. Hook into Spark's write path using a `QueryExecutionListener` that fires after a Parquet write completes and patches the footer.

### Files to Create/Modify

| File | Action | Purpose |
|---|---|---|
| `parquet/GeoParquetWriter.scala` | Create | Builds the `geo` JSON blob from DataFrame schema + detects WKB columns |
| `parquet/GeoParquetWriteRule.scala` | Create | Optimizer rule that detects `InsertIntoHadoopFsRelationCommand` writing Parquet with geometry columns and sets writer options |
| `CometSparkSessionExtensions.scala` | Modify | Add `injectOptimizerRule` for `GeoParquetWriteRule` |

### Implementation Steps

1. **Detect geometry columns at write time**
   - In optimizer rule, match `InsertIntoHadoopFsRelationCommand` where `fileFormat` is `ParquetFileFormat`
   - Check if any output column is `BinaryType` and the plan contains `StGeomFromWkb` or column name is a known geo column
   - Build `GeoColumnInfo` for each geometry column (encoding=WKB, geometry_types inferred from schema metadata if present)

2. **Build `geo` JSON**
   ```json
   {
     "version": "1.1.0",
     "primary_column": "geometry",
     "columns": {
       "geometry": {
         "encoding": "WKB",
         "geometry_types": [],
         "bbox": [-180.0, -90.0, 180.0, 90.0]
       }
     }
   }
   ```

3. **Inject into Parquet footer**
   - Use `ParquetFileWriter` / `ParquetOutputFormat` extra metadata option: `spark.hadoop.parquet.extra.metadata` map
   - Or post-write: rewrite footer with `ParquetFileWriter.appendTo` and `ExtraMetaData`
   - Simplest: set `spark.sql.parquet.writeLegacyFormat` extra options to pass metadata through `ParquetOutputFormat.JOB_SUMMARY_LEVEL`

4. **Register**
   - Wire into `CometSparkSessionExtensions` via `injectOptimizerRule`

### Test
```scala
val df = spark.read.parquet("/tmp/example.parquet")
df.write.parquet("/tmp/out_geo.parquet")
val meta = GeoParquetFunctions.readMetadata(spark, "/tmp/out_geo.parquet/part-00000-*.parquet")
assert(meta.isDefined)
assert(meta.get.columns.contains("geometry"))
```

---

## Phase 2 — Spatial Partitioning (Geohash / H3)

**Goal:** Add `st_geohash(geom, precision)` and `st_h3_cell(geom, resolution)` functions so users can partition GeoParquet files spatially for faster range queries.

**Why it matters:** Without spatial partitioning, every ST_Intersects query does a full table scan. With geohash/H3 partitioning, Spark can prune partitions using the bbox of the query geometry.

### New Rust Functions Needed

| Function | Signature | Rust crate |
|---|---|---|
| `st_geohash` | `(geom: WKB, precision: i32) -> String` | `geohash` crate |
| `st_h3_cell` | `(geom: WKB, resolution: i32) -> i64` | `h3o` crate |
| `st_h3_cell_to_geom` | `(cell: i64) -> WKB` | `h3o` crate |
| `st_geohash_to_geom` | `(hash: String) -> WKB` | `geohash` crate |

### Files to Create/Modify

| File | Action |
|---|---|
| `native/core/src/execution/expressions/geo/st_geohash.rs` | Create |
| `native/core/src/execution/expressions/geo/st_h3_cell.rs` | Create |
| `native/core/src/execution/expressions/geo/st_h3_cell_to_geom.rs` | Create |
| `native/core/src/execution/expressions/geo/st_geohash_to_geom.rs` | Create |
| `native/core/src/execution/expressions/geo/mod.rs` | Add new modules |
| `native/core/src/execution/planner/expression_registry.rs` | Register new UDFs |
| `native/Cargo.toml` | Add `geohash` and `h3o` dependencies |
| `GeoExpressions.scala` | Add case classes + desc() infos |
| `CometGeoFallback.scala` | Add stubs |
| `serde/geo.scala` | Add serde map entries |
| `CometSparkSessionExtensions.scala` | Add injectFunction calls |

### Partition Usage Example
```scala
// Write partitioned by geohash
df.withColumn("geohash", st_geohash($"geometry", lit(5)))
  .write
  .partitionBy("geohash")
  .parquet("/tmp/geo_partitioned/")

// Read with partition pruning (Spark auto-prunes geohash= partitions)
spark.read.parquet("/tmp/geo_partitioned/")
  .where("geohash = 'u4pru'")
  .select(st_area($"geometry"))
  .show()
```

---

## Phase 3 — Spatial Join Optimization

**Goal:** Detect `st_intersects(a.geom, b.geom)` or `st_contains(a.geom, b.geom)` in join conditions and route them to a native R-tree-indexed spatial join instead of a Cartesian/hash join.

**Why it matters:** A naive join of two 1M-row geometry datasets is 10^12 comparisons. An R-tree spatial join is O(n log n).

**This is the most complex phase — requires new Rust operator.**

### Architecture

```
Spark logical plan:
  Join(condition=st_intersects(a.geom, b.geom))

After CometSpatialJoinRule:
  CometSpatialJoinExec(
    left=CometScanExec(a),
    right=CometScanExec(b),
    leftKey=a.geom,
    rightKey=b.geom,
    predicate=Intersects
  )

Native Rust execution:
  SpatialJoinExec {
    - Builds R-tree index on right side
    - For each left geometry, queries R-tree for candidates
    - Applies precise predicate check on candidates only
  }
```

### Files to Create/Modify

| File | Action |
|---|---|
| `rules/CometSpatialJoinRule.scala` | Create — detects spatial join patterns |
| `spark/src/main/scala/org/apache/comet/execution/CometSpatialJoinExec.scala` | Create — physical operator |
| `serde/operator/CometSpatialJoinSerde.scala` | Create — serialize to proto |
| `native/proto/src/proto/operator.proto` | Add `SpatialJoin` message |
| `native/core/src/execution/operators/spatial_join.rs` | Create — R-tree join impl |
| `native/core/src/execution/planner.rs` | Handle `SpatialJoin` operator |
| `native/Cargo.toml` | Add `rstar` crate for R-tree |

### Rust Dependencies
- `rstar = "0.12"` — R-tree spatial indexing
- `geo = "0.28"` — already used for geometry operations

### Implementation Complexity: HIGH
Estimated: 3-4 weeks of Rust + Scala work.

---

## Phase 4 — More ST Functions

Functions worth adding (all pure Rust, no new operator needed):

| Function | Description | Rust impl |
|---|---|---|
| `st_collect(geoms)` | Aggregate: collect geometries into GeometryCollection | New aggregate UDF |
| `st_snap(a, b, tolerance)` | Snap geometry A to geometry B | `geo` crate |
| `st_polygonize(lines)` | Build polygons from a set of lines | `geo` crate |
| `st_voronoi(points)` | Voronoi diagram from point collection | `geo` crate |
| `st_delaunay(points)` | Delaunay triangulation | `geo` crate |
| `st_minimum_bounding_circle(geom)` | Smallest enclosing circle | `geo` crate |
| `st_affine(geom, a,b,c,d,e,f)` | 2D affine transform | `geo` crate |
| `st_rotate(geom, angle)` | Rotate geometry | `geo` crate |
| `st_scale(geom, sx, sy)` | Scale geometry | `geo` crate |
| `st_normalize(geom)` | Normalize geometry to canonical form | `geo` crate |
| `st_force_2d(geom)` | Drop Z coordinate | `geo` crate |
| `st_z(geom)` | Extract Z coordinate of point | `geo` crate |
| `st_zm(geom)` | Extract ZM coordinates | `geo` crate |

---

## Recommended Build Order

```
Phase 1: Write GeoParquet          (1-2 days, Scala only)
    ↓
Phase 2: st_geohash + st_h3_cell   (2-3 days, Rust + Scala)
    ↓
Phase 4: More ST functions         (1 day each, repeatable pattern)
    ↓
Phase 3: Spatial join              (3-4 weeks, complex Rust)
```

Start with Phase 1 — it has the highest user-visible impact for the least implementation complexity.

---

## Key Constraints

- EC2 build machine has no `git push` access — all commits must be made locally and pushed from this machine
- Do not run `cargo build` or `mvn build` locally — EC2 is build-only
- Never touch `geo-window-native-exec` branch
- `geo-parquet` is the current working branch and source of truth
