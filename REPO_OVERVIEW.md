# Datafusion-Comet Repository Overview

Apache Spark plugin that replaces the JVM execution engine with Apache DataFusion (Rust)
for native columnar query execution. Geo extensions add 53 spatial ST_ functions backed
by native Rust UDFs.

---

## Top-Level Structure

```
datafusion-comet/
├── spark/          Scala/Java — Spark integration layer (Maven)
├── native/         Rust — DataFusion native execution engine (Cargo)
├── common/         Shared Java/Scala utilities
├── benchmarks/     TPC-DS benchmarking suite
├── docs/           Documentation sources
├── pom.xml         Maven root build
├── Makefile        Build orchestration (make release PROFILES=...)
└── rust-toolchain.toml
```

---

## End-to-End Query Flow

```
User SQL / DataFrame API
        │
        ▼
┌───────────────────────┐
│   Spark SQL Parser    │  Catalyst AST
└───────────────────────┘
        │
        ▼
┌───────────────────────┐
│  Catalyst Analyzer    │  Resolves names, types, binds functions
│                       │  injectFunction() puts ST_ funcs in catalog
└───────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────┐
│           Catalyst Optimizer                       │
│                                                   │
│  injectOptimizerRule:                             │
│  ├── CometGeoParquetRule    auto-wrap WKB cols    │
│  └── CometGeoPreAggregateRule  lift geo above     │
│                               shuffle             │
└───────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────┐
│  Spark Physical Plan  │  ProjectExec, FilterExec,
│  (row-based)          │  HashAggregateExec, etc.
└───────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────┐
│  CometScanRule  (injectQueryStagePrepRule)         │
│  Parquet/CSV/Iceberg scans → CometScanExec        │
└───────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────┐
│  CometExecRule  (injectQueryStagePrepRule)         │
│  ProjectExec       → CometProjectExec             │
│  FilterExec        → CometFilterExec              │
│  HashAggregateExec → CometHashAggregateExec       │
│  BroadcastHashJoin → CometBroadcastHashJoinExec   │
│  SortMergeJoin     → CometSortMergeJoinExec       │
│  WindowExec        → CometWindowExec              │
│                                                   │
│  Validates: can all expressions be serialized?    │
│  If not → leave as Spark (fallback)               │
└───────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────┐
│  EliminateRedundantTransitions                    │
│  Insert ColumnarToRow / RowToColumnar adapters    │
│  Remove back-to-back redundant transitions        │
└───────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────┐
│  QueryPlanSerde  (on operator execution)          │
│  Comet physical plan → protobuf binary            │
│                                                   │
│  Scala expressions → proto Expr messages          │
│  StArea(child) → ScalarFunc{name="st_area",       │
│                             args=[child]}         │
│  serde/geo.scala maps each ST_ class → funcName   │
└───────────────────────────────────────────────────┘
        │  protobuf bytes over JNI
        ▼
┌───────────────────────────────────────────────────┐
│  Rust JNI Entry Point                             │
│  native/core/src/execution/jni_api.rs             │
│                                                   │
│  Deserialize protobuf Operator                    │
│        │                                          │
│        ▼                                          │
│  PhysicalPlanner (planner.rs)                     │
│  proto Operator → DataFusion PhysicalPlan         │
│  proto Expr     → DataFusion PhysicalExpr         │
│        │                                          │
│        ▼                                          │
│  ExpressionRegistry                               │
│  "st_area" → st_area::make_udf()                  │
│  "st_contains" → st_contains::make_udf()          │
│        │                                          │
│        ▼                                          │
│  DataFusion Execution Engine                      │
│  Columnar Arrow RecordBatch processing            │
└───────────────────────────────────────────────────┘
        │  Arrow RecordBatch (zero-copy)
        ▼
┌───────────────────────┐
│  ColumnarToRowExec    │  Arrow → Spark InternalRow
└───────────────────────┘
        │
        ▼
     Result
```

---

## Scala Layer (`spark/src/main/scala/org/apache/comet/`)

```
CometSparkSessionExtensions.scala   ← ENTRY POINT
│   registers everything below into Spark
│
├── expressions/
│   ├── GeoExpressions.scala        53 ST_ case classes + ExpressionInfo descriptors
│   └── CometGeoFallback.scala      JVM fallback stubs (used when Comet disabled)
│
├── parquet/
│   ├── GeoParquetMetadata.scala    Parse GeoParquet "geo" JSON footer key
│   ├── GeoParquetFunctions.scala   st_geoparquet_metadata / st_geoparquet_columns UDFs
│   └── CometParquetUtils.scala     Parquet reader utilities
│
├── rules/
│   ├── CometGeoParquetRule.scala   Auto-detect WKB cols, inject StGeomFromWkb
│   ├── CometGeoPreAggregateRule.scala  Lift geo exprs above shuffle
│   ├── CometExecRule.scala         Convert Spark exec → Comet exec (44KB)
│   ├── CometScanRule.scala         Convert scans → CometScanExec (36KB)
│   ├── RewriteJoin.scala           SortMergeJoin → ShuffledHashJoin
│   └── EliminateRedundantTransitions.scala
│
└── serde/
    ├── geo.scala                   ST_ expression → ScalarFunc proto mapping
    ├── QueryPlanSerde.scala        Main plan → protobuf serializer (35KB)
    ├── arithmetic.scala            Arithmetic expressions
    ├── strings.scala               String expressions
    └── operator/
        └── CometDataWritingCommand.scala   Parquet write serialization
```

---

## Rust Layer (`native/core/src/`)

```
lib.rs / jni_api.rs                 ← JNI ENTRY POINT
│
execution/
├── planner.rs                      proto Operator/Expr → DataFusion plan
├── planner/
│   ├── expression_registry.rs      "st_area" → st_area::make_udf() HashMap
│   └── operator_registry.rs        operator enum → constructor HashMap
│
├── expressions/
│   ├── arithmetic.rs, strings.rs, temporal.rs, ...
│   └── geo/                        53 native geo UDF implementations
│       ├── mod.rs                  registers all geo UDFs
│       ├── wkb_util.rs             WKB encode/decode helpers
│       │
│       ├── Spatial predicates (→ bool)
│       │   st_contains, st_intersects, st_within, st_covers, st_covered_by,
│       │   st_equals, st_touches, st_crosses, st_disjoint, st_overlaps
│       │
│       ├── Measurements (→ f64)
│       │   st_area, st_distance, st_distance_sphere, st_length,
│       │   st_perimeter, st_hausdorff_distance
│       │
│       ├── Constructors (→ WKB binary)
│       │   st_geom_from_wkt, st_geom_from_wkb, st_geom_from_geojson,
│       │   st_point, st_make_line, st_make_envelope
│       │
│       ├── Serializers (→ string/binary)
│       │   st_as_text, st_as_binary, st_as_geojson
│       │
│       ├── Accessors (→ scalar)
│       │   st_geometry_type, st_is_empty, st_is_valid, st_dimension,
│       │   st_num_points, st_num_geometries, st_srid, st_x, st_y
│       │
│       ├── Transformations (→ WKB)
│       │   st_centroid, st_envelope, st_convex_hull, st_buffer,
│       │   st_simplify, st_simplify_preserve_topology, st_boundary,
│       │   st_flip_coordinates, st_translate, st_endpoint,
│       │   st_rings, st_union, st_intersection, st_difference,
│       │   st_sym_difference
│       │
│       └── New (added this branch)
│           st_geom_from_wkb, st_as_binary, st_is_valid, st_srid,
│           st_dimension, st_num_geometries, st_startpoint, st_endpoint,
│           st_exteriorring, st_numinteriorrings, st_translate
│
└── operators/
    ├── scan.rs                     Table scan operator
    ├── parquet_writer.rs           Parquet output operator
    ├── iceberg_scan.rs             Iceberg scan
    └── ...
```

---

## Protobuf Layer (`native/proto/src/proto/`)

```
expr.proto        Expr { oneof { ScalarFunc, BinaryExpr, Cast, Literal, ... } }
                  Geo functions use: ScalarFunc { name: "st_area", args: [...] }

operator.proto    Operator { oneof { Scan, Project, Filter, Aggregate,
                            HashJoin, SortMergeJoin, Window, Exchange, ... } }

types.proto       DataType, Field, Schema (Arrow types)
literal.proto     Literal values
partitioning.proto  Hash/Range partitioning schemes
metric.proto      Execution metrics tree
config.proto      Runtime configuration options
```

---

## GeoParquet Feature (This Branch)

```
READ PATH:
  Parquet file with "geo" footer key
        │
        ▼
  CometGeoParquetRule (optimizer)
  ├── reads Parquet footer via ParquetFileReader
  ├── caches result in footerCache (ConcurrentHashMap)
  ├── detects WKB-encoded geometry columns
  └── injects Project(Alias(StGeomFromWkb(col), col), LogicalRelation)
        │
        ▼
  Downstream ST_ functions see geometry as already-decoded WKB
  (no manual st_geomfromwkb needed)

METADATA UDFs (registered via injectCheckRule at session init):
  st_geoparquet_metadata(path) → raw "geo" JSON string
  st_geoparquet_columns(path)  → sorted array of WKB column names

WRITE PATH: (Phase 1 — not yet implemented)
  df.write.parquet(path)
        │
        ▼
  [TODO] GeoParquetWriteRule detects geometry columns
  [TODO] Injects "geo" JSON into Parquet footer metadata
```

---

## How to Add a New ST_ Function (Pattern)

```
1. Rust:   native/core/src/execution/expressions/geo/st_new_func.rs
           implement make_udf() returning Arc<ScalarUDF>

2. Rust:   native/core/src/execution/expressions/geo/mod.rs
           add: pub mod st_new_func;

3. Rust:   native/core/src/execution/planner/expression_registry.rs
           register: "st_new_func" -> st_new_func::make_udf()

4. Scala:  GeoExpressions.scala
           add case class StNewFunc(...) extends Expression with CometGeoExpression
           add val stNewFuncInfo = desc("st_new_func", ...)

5. Scala:  CometGeoFallback.scala
           add def newFunc(...): Array[Byte] = notSupported("st_new_func")

6. Scala:  serde/geo.scala
           add classOf[StNewFunc] -> new CometGeoScalarFunc("st_new_func")

7. Scala:  CometSparkSessionExtensions.scala
           add extensions.injectFunction(GeoExpressions.stNewFuncInfo)
```

---

## Branch History Summary

```
main
  └── geo-window-native-exec    Window function native execution
        └── geo-parquet  ← CURRENT WORKING BRANCH
              ├── feat: 11 new geo functions (st_geomfromwkb, st_asbinary, etc.)
              ├── feat: CometGeoParquetRule (auto-detect + auto-wrap WKB cols)
              ├── feat: GeoParquetMetadata parser
              ├── feat: st_geoparquet_metadata / st_geoparquet_columns UDFs
              └── fix:  SerializableConfiguration, idempotent rule, eager UDF reg
```

---

## Build Commands (EC2 only)

```bash
# Full build
make release PROFILES="-Pspark-3.5 -Pscala-2.13"

# Scala only (faster, skips Rust)
mvn package -pl spark -am -DskipTests -Pspark-3.5 -Pscala-2.13 -q

# Deploy JAR to Spark
cp spark/target/comet-spark-spark3.5_2.13-0.17.0-SNAPSHOT.jar ~/spark/jars/

# Quick smoke test
spark-shell --jars spark/target/comet-spark-spark3.5_2.13-0.17.0-SNAPSHOT.jar \
  --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
  --conf spark.comet.enabled=true
```
