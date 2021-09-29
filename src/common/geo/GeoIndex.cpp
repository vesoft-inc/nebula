/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This sourc_e code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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

storage::cpp2::IndexColumnHint ScanRange::toIndexColumnHint() {
  storage::cpp2::IndexColumnHint hint;
  // set_column_name should be called later
  if (isRangeScan) {
    hint.set_scan_type(storage::cpp2::ScanType::RANGE);
    hint.set_begin_value(
        IndexKeyUtils::encodeUint64(rangeMin));  // Encode uint64_t as string ahead of time
    hint.set_end_value(IndexKeyUtils::encodeUint64(rangeMax));
  } else {
    hint.set_scan_type(storage::cpp2::ScanType::PREFIX);
    hint.set_begin_value(IndexKeyUtils::encodeUint64(rangeMin));
  }
  return hint;
}

std::vector<uint64_t> GeographyIndex::indexCells(const Geography& g) const noexcept {
  auto cells = coveringCells(g);
  std::vector<uint64_t> cellIds;
  cellIds.reserve(cells.size());
  for (auto& cell : cells) {
    cellIds.push_back(cell.id());
  }
  return cellIds;
}

std::vector<ScanRange> GeographyIndex::dWithin(const Geography& g, double distance) const noexcept {
  auto r = g.asS2();
  // TODO(jie): Better to ensure the Geography value is valid to build s2 when constructing.
  if (!r) {
    LOG(INFO) << "GeographyIndex::dWithin(), asS2() failed.";
    return {};
  }

  S1Angle radius = S2Earth::ToAngle(util::units::Meters(distance));
  switch (g.shape()) {
    case GeoShape::POINT: {
      const S2Point& gPoint = static_cast<S2PointRegion*>(r.get())->point();
      S2Cap gCap(gPoint, radius);
      auto cells = coveringCells(gCap);
      return scanRange(cells);
    }
    case GeoShape::LINESTRING: {
      S2Polyline* gLine = static_cast<S2Polyline*>(r.get());
      MutableS2ShapeIndex index;
      index.Add(std::make_unique<S2Polyline::Shape>(gLine));
      S2ShapeIndexBufferedRegion gBuffer(&index, radius);
      auto cells = coveringCells(gBuffer);
      return scanRange(cells);
    }
    case GeoShape::POLYGON: {
      S2Polygon* gPolygon = static_cast<S2Polygon*>(r.get());
      S2ShapeIndexBufferedRegion gBuffer(&gPolygon->index(), radius);
      auto cells = coveringCells(gBuffer);
      return scanRange(cells);
    }
    default:
      LOG(FATAL)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return {};
  }
}

std::vector<S2CellId> GeographyIndex::coveringCells(const Geography& g) const noexcept {
  auto r = g.asS2();
  // TODO(jie): Better to ensure the Geography value is valid to build s2 when constructing.
  if (!r) {
    LOG(INFO) << "GeographyIndex::coveringCells(), asS2() failed.";
    return {};
  }

  // Currently region coverer options doesn't work for point. Point always use level 30.
  if (g.shape() == GeoShape::POINT) {
    const S2Point& gPoint = static_cast<S2PointRegion*>(r.get())->point();
    return {S2CellId(gPoint)};
  }

  S2RegionCoverer rc(rcParams_.s2RegionCovererOpts());
  std::vector<S2CellId> covering;
  rc.GetCovering(*r, &covering);
  // 1. NO NEED TO CALL S2RegionCoverer::CanonicalizeCovering(covering), because the covering is
  // already canonical, which means that is sorted, non-overlapping and satisfy the desired
  // constraints min_level, max_level.
  // 2. DO NOT CALL S2CellUnion::Normalize(covering), it will replacing groups of 4 child cells by
  // their parent cell, In this case, it may cause the covering don't satisfy the desired
  // constraints min_level.
  return covering;
}

std::vector<S2CellId> GeographyIndex::coveringCells(const S2Region& r) const noexcept {
  S2RegionCoverer rc(rcParams_.s2RegionCovererOpts());
  std::vector<S2CellId> covering;
  rc.GetCovering(r, &covering);
  return covering;
}

std::vector<S2CellId> GeographyIndex::ancestorCells(
    const std::vector<S2CellId>& cells) const noexcept {
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
  // The ancestors here is non-overlapping but unsorted. Do we need sort it?
  // May need to call S2RegionCoverer::CanonicalizeCovering(&ancestors)?
  return ancestors;
}

std::vector<ScanRange> GeographyIndex::scanRange(
    const std::vector<S2CellId>& cells) const noexcept {
  std::vector<ScanRange> scanRanges;
  for (const S2CellId& cellId : cells) {
    if (cellId.is_leaf()) {
      scanRanges.emplace_back(cellId.id());
    } else {
      scanRanges.emplace_back(cellId.range_min().id(), cellId.range_max().id());
    }
  }

  if (!pointsOnly_) {
    auto ancestors = ancestorCells(cells);
    for (const S2CellId& cellId : ancestors) {
      scanRanges.emplace_back(cellId.id());
    }
  }

  return scanRanges;
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<S2CellId>::operator()(const S2CellId& c) const noexcept {
  return hash<uint64_t>{}(c.id());
}

}  // namespace std
