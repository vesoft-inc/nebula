/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/function/S2Util.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2shape_index_buffered_region.h>

namespace nebula {

uint64_t s2CellIdFromPoint(const Geography& a, int level) {
  auto aRegion = a.asS2();
  if (!aRegion) {
    return -1;
  }
  if (level < 0 || level > 30) {
    LOG(ERROR) << "s2 level argument must be in range [0,30], got " << level;
    return -1;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      const auto& s2Point = static_cast<S2PointRegion*>(aRegion.get())->point();
      S2CellId cellId(s2Point);
      return level == 30 ? cellId.id() : cellId.parent(level).id();
    }
    case GeoShape::LINESTRING:
    case GeoShape::POLYGON:
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR) << "S2_CellIdFromPoint only accept Point";
      return -1;
    }
  }

  return false;
}

std::vector<uint64_t> s2CoveringCellIds(
    const Geography& a, int minLevel, int maxLevel, int maxCells, double bufferInMeters) {
  auto aRegion = a.asS2();
  if (!aRegion) {
    return {};
  }
  if (minLevel < 0 || minLevel > 30) {
    LOG(ERROR) << "s2 min_level argument must be in range [0,30], got " << minLevel;
    return {};
  }
  if (maxLevel < 0 || maxLevel > 30) {
    LOG(ERROR) << "s2 max_level argument must be in range [0,30], got " << maxLevel;
    return {};
  }
  if (maxCells <= 0) {
    LOG(ERROR) << "s2 max_cells argument must be greater than or equal to 0, got " << maxCells;
    return {};
  }
  if (bufferInMeters < 0.0) {
    LOG(ERROR) << "s2 buffer_meters argument must be nonnegative, got " << bufferInMeters;
    return {};
  }

  S2RegionCoverer::Options opts;
  opts.set_min_level(minLevel);
  opts.set_max_level(maxLevel);
  opts.set_max_cells(maxCells);

  if (bufferInMeters == 0.0) {
    return coveringCellIds(*aRegion, opts);
  }

  S1Angle radius = S2Earth::ToAngle(util::units::Meters(bufferInMeters));

  switch (a.shape()) {
    case GeoShape::POINT: {
      const S2Point& gPoint = static_cast<S2PointRegion*>(aRegion.get())->point();
      S2Cap gCap(gPoint, radius);
      return coveringCellIds(gCap, opts);
    }
    case GeoShape::LINESTRING: {
      S2Polyline* gLine = static_cast<S2Polyline*>(aRegion.get());
      MutableS2ShapeIndex index;
      index.Add(std::make_unique<S2Polyline::Shape>(gLine));
      S2ShapeIndexBufferedRegion gBuffer(&index, radius);
      return coveringCellIds(gBuffer, opts);
    }
    case GeoShape::POLYGON: {
      S2Polygon* gPolygon = static_cast<S2Polygon*>(aRegion.get());
      S2ShapeIndexBufferedRegion gBuffer(&gPolygon->index(), radius);
      return coveringCellIds(gBuffer, opts);
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return {};
    }
  }

  return {};
}

std::vector<uint64_t> coveringCellIds(const S2Region& r, const S2RegionCoverer::Options& opts) {
  S2RegionCoverer rc(opts);
  std::vector<S2CellId> covering;
  rc.GetCovering(r, &covering);
  std::vector<uint64_t> cellIds;
  cellIds.reserve(covering.size());
  for (auto& cellId : covering) {
    cellIds.push_back(cellId.id());
  }
  return cellIds;
}

}  // namespace nebula
