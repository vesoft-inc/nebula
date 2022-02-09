/*
 *
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 *
 */

#include "common/geo/GeoIndex.h"

#include <folly/String.h>
#include <folly/hash/Hash.h>
#include <s2/mutable_s2shape_index.h>
#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2shape_index_buffered_region.h>
#include <s2/util/units/length-units.h>

#include <cstdint>

#include "common/datatypes/Geography.h"
#include "common/utils/IndexKeyUtils.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace geo {

bool ScanRange::operator==(const ScanRange& rhs) const {
  if (isRangeScan != rhs.isRangeScan) {
    return false;
  }
  if (isRangeScan) {
    return rangeMin == rhs.rangeMin && rangeMax == rhs.rangeMax;
  }
  return rangeMin == rhs.rangeMin;
}

nebula::storage::cpp2::IndexColumnHint ScanRange::toIndexColumnHint() const {
  nebula::storage::cpp2::IndexColumnHint hint;
  // column_name should be set by the caller
  if (isRangeScan) {
    hint.scan_type_ref() = nebula::storage::cpp2::ScanType::RANGE;
    // Encode uint64_t as string in advance
    hint.begin_value_ref() = IndexKeyUtils::encodeUint64(rangeMin);
    hint.end_value_ref() = IndexKeyUtils::encodeUint64(rangeMax);
    hint.include_begin_ref() = true;
    hint.include_end_ref() = true;
  } else {
    hint.scan_type_ref() = nebula::storage::cpp2::ScanType::PREFIX;
    hint.begin_value_ref() = IndexKeyUtils::encodeUint64(rangeMin);
  }
  return hint;
}

std::vector<uint64_t> GeoIndex::indexCells(const Geography& g) const noexcept {
  auto r = g.asS2();
  if (UNLIKELY(!r)) {
    return {};
  }

  auto cells = coveringCells(*r, g.shape() == GeoShape::POINT);
  std::vector<uint64_t> cellIds;
  cellIds.reserve(cells.size());
  for (auto& cell : cells) {
    cellIds.push_back(cell.id());
  }
  return cellIds;
}

std::vector<ScanRange> GeoIndex::intersects(const Geography& g) const noexcept {
  auto r = g.asS2();
  if (UNLIKELY(!r)) {
    return {};
  }

  return intersects(*r, g.shape() == GeoShape::POINT);
}

// covers degenerates to intersects currently
std::vector<ScanRange> GeoIndex::covers(const Geography& g) const noexcept {
  return intersects(g);
}

// coveredBy degenerates to intersects currently
std::vector<ScanRange> GeoIndex::coveredBy(const Geography& g) const noexcept {
  return intersects(g);
}

std::vector<ScanRange> GeoIndex::dWithin(const Geography& g, double distance) const noexcept {
  auto r = g.asS2();
  if (UNLIKELY(!r)) {
    return {};
  }

  S1Angle radius = S2Earth::ToAngle(util::units::Meters(distance));
  // First expand the region, then build the covering
  switch (g.shape()) {
    case GeoShape::POINT: {
      const S2Point& gPoint = static_cast<S2PointRegion*>(r.get())->point();
      S2Cap gCap(gPoint, radius);
      return intersects(gCap);
    }
    case GeoShape::LINESTRING: {
      S2Polyline* gLine = static_cast<S2Polyline*>(r.get());
      MutableS2ShapeIndex index;
      index.Add(std::make_unique<S2Polyline::Shape>(gLine));
      S2ShapeIndexBufferedRegion gBuffer(&index, radius);
      return intersects(gBuffer);
    }
    case GeoShape::POLYGON: {
      S2Polygon* gPolygon = static_cast<S2Polygon*>(r.get());
      S2ShapeIndexBufferedRegion gBuffer(&gPolygon->index(), radius);
      return intersects(gBuffer);
    }
    default:
      LOG(FATAL)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return {};
  }
}

std::vector<ScanRange> GeoIndex::intersects(const S2Region& r, bool isPoint) const noexcept {
  auto cells = coveringCells(r, isPoint);
  std::vector<ScanRange> scanRanges;
  for (const S2CellId& cellId : cells) {
    if (cellId.is_leaf()) {
      scanRanges.emplace_back(cellId.id());
    } else {
      scanRanges.emplace_back(cellId.range_min().id(), cellId.range_max().id());
    }
  }

  // For the indexed column which only contains point, we don't need to get the ancestor cells,
  // because the point is at max level(30).
  if (!pointsOnly_) {
    auto ancestors = ancestorCells(cells);
    for (const S2CellId& cellId : ancestors) {
      scanRanges.emplace_back(cellId.id());
    }
  }

  return scanRanges;
}

std::vector<S2CellId> GeoIndex::coveringCells(const S2Region& r, bool isPoint) const noexcept {
  // Currently we don't apply region coverer params to point, because it's useless.
  // Point always use level 30.
  if (isPoint) {
    const S2Point& gPoint = static_cast<const S2PointRegion*>(&r)->point();
    return {S2CellId(gPoint)};
  }
  S2RegionCoverer rc(rcParams_.s2RegionCovererOpts());
  std::vector<S2CellId> covering;
  rc.GetCovering(r, &covering);
  // 1. NO NEED TO CALL S2RegionCoverer::CanonicalizeCovering(covering), because the covering is
  // already canonical, which means that is sorted, non-overlapping and satisfy the desired
  // constraints min_level, max_level.
  // 2. DO NOT CALL S2CellUnion::Normalize(covering), it will replacing groups of 4 child cells by
  // their parent cell, In this case, it may cause the covering don't satisfy the desired
  // constraints min_level.
  return covering;
}

std::vector<S2CellId> GeoIndex::ancestorCells(const std::vector<S2CellId>& cells) const noexcept {
  // DCHECK(rc.IsCanonical(cells));
  std::vector<S2CellId> ancestors;
  std::unordered_set<S2CellId> seen;
  for (const auto& cellId : cells) {
    for (auto l = cellId.level() - 1; l >= rcParams_.minCellLevel_; --l) {
      S2CellId parentCellId = cellId.parent(l);
      if (seen.find(parentCellId) != seen.end()) {
        break;
      }
      seen.emplace(parentCellId);
      ancestors.push_back(parentCellId);
    }
  }
  // The ancestors here is non-overlapping but unsorted. Do we need to sort it?
  // May need to call S2RegionCoverer::CanonicalizeCovering(&ancestors)?
  return ancestors;
}

}  // namespace geo
}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<S2CellId>::operator()(const S2CellId& c) const noexcept {
  return hash<uint64_t>{}(c.id());
}

}  // namespace std
