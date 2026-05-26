// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared WKB encode/decode utilities and geo-types conversion for Comet geo UDFs.
//!
//! Ported from sedona-db/rust/sedona-geo/src/to_geo.rs (Apache-2.0).

use datafusion::common::{DataFusionError, Result as DataFusionResult};
use geo_traits::{
    to_geo::{
        ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon, ToGeoPoint,
        ToGeoPolygon,
    },
    GeometryCollectionTrait, GeometryTrait,
    GeometryType::*,
};
use geo_types::Geometry;
use wkb::{
    reader::Wkb,
    writer::{write_geometry, WriteOptions},
    Endianness,
};

/// Standard WKB write options used across all Comet geo UDFs (little-endian).
pub const WKB_WRITE_OPTS: WriteOptions = WriteOptions {
    endianness: Endianness::LittleEndian,
};

/// Decode raw WKB bytes into a `Wkb<'_>` view (zero-copy).
#[inline]
pub fn read_wkb(bytes: &[u8]) -> DataFusionResult<Wkb<'_>> {
    wkb::reader::read_wkb(bytes).map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Encode a `geo_types::Geometry` into a WKB byte vector.
#[inline]
pub fn geom_to_wkb(geom: &Geometry) -> DataFusionResult<Vec<u8>> {
    let mut buf = Vec::new();
    write_geometry(&mut buf, geom, &WKB_WRITE_OPTS)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(buf)
}

/// Convert a `Wkb<'_>` (or any `GeometryTrait`) into a `geo_types::Geometry`.
///
/// Ported from sedona-db `to_geo.rs` to work around a Rust compiler recursion
/// issue in release mode with geo-traits' own `to_geometry()`.
pub fn wkb_to_geo(wkb: Wkb<'_>) -> DataFusionResult<Geometry> {
    to_geometry(wkb).ok_or_else(|| {
        DataFusionError::Execution(
            "unsupported geometry: POINT EMPTY, MULTIPOINT with EMPTY child, \
             or deeply nested GEOMETRYCOLLECTION"
                .into(),
        )
    })
}

fn to_geometry(item: impl GeometryTrait<T = f64>) -> Option<Geometry> {
    match item.as_type() {
        Point(geom) => geom.try_to_point().map(Geometry::Point),
        LineString(geom) => Some(Geometry::LineString(geom.to_line_string())),
        Polygon(geom) => Some(Geometry::Polygon(geom.to_polygon())),
        MultiPoint(geom) => geom.try_to_multi_point().map(Geometry::MultiPoint),
        MultiLineString(geom) => Some(Geometry::MultiLineString(geom.to_multi_line_string())),
        MultiPolygon(geom) => Some(Geometry::MultiPolygon(geom.to_multi_polygon())),
        GeometryCollection(geom) => geometry_collection_to_geometry(geom),
        _ => None,
    }
}

fn geometry_collection_to_geometry<GC: GeometryCollectionTrait<T = f64>>(
    geom: &GC,
) -> Option<Geometry> {
    let geometries = geom
        .geometries()
        .filter_map(|child| match child.as_type() {
            Point(geom) => geom.try_to_point().map(Geometry::Point),
            LineString(geom) => Some(Geometry::LineString(geom.to_line_string())),
            Polygon(geom) => Some(Geometry::Polygon(geom.to_polygon())),
            MultiPoint(geom) => geom.try_to_multi_point().map(Geometry::MultiPoint),
            MultiLineString(geom) => Some(Geometry::MultiLineString(geom.to_multi_line_string())),
            MultiPolygon(geom) => Some(Geometry::MultiPolygon(geom.to_multi_polygon())),
            GeometryCollection(inner) => geometry_collection_to_geometry(inner),
            _ => None,
        })
        .collect::<Vec<_>>();

    if geometries.len() != geom.num_geometries() {
        return None;
    }

    Some(Geometry::GeometryCollection(geo_types::GeometryCollection(
        geometries,
    )))
}

/// Iterate over a `BinaryArray` column and decode each row as `Wkb<'_>`,
/// applying `f` for each `Some(wkb)` / `None` pair.
#[macro_export]
macro_rules! iter_wkb {
    ($col:expr, $f:expr) => {{
        $col.iter()
            .map(|bytes| match bytes {
                Some(b) => $crate::execution::expressions::geo::wkb_util::read_wkb(b)
                    .ok()
                    .and_then($f),
                None => None,
            })
    }};
}
