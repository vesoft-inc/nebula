/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_GEO_GEOINDEX_H
#define COMMON_GEO_GEOINDEX_H

#include <s2/s2region_coverer.h>

#include <algorithm>
#include <vector>

#include "common/datatypes/Geography.h"

namespace nebula {

namespace storage {
namespace cpp2 {
class IndexColumnHint;
}  // namespace cpp2
}  // namespace storage

namespace geo {

struct RegionCoverParams {
  // TODO(jie): Find the best default params
  int minCellLevel_ = 0;
  int maxCellLevel_ = 30;  // About 1m
  int maxCellNum_ = 8;

  RegionCoverParams() = default;

  RegionCoverParams(int minLevl, int maxLevl, int maxCells)
      : minCellLevel_(minLevl), maxCellLevel_(maxLevl), maxCellNum_(maxCells) {}

  S2RegionCoverer::Options s2RegionCovererOpts() const {
    S2RegionCoverer::Options opts;
    opts.set_min_level(minCellLevel_);
    opts.set_max_level(maxCellLevel_);
    opts.set_max_cells(maxCellNum_);
    return opts;
  }
};

// scan type: PREFIX or RANGE
struct ScanRange {
  uint64_t rangeMin;
  uint64_t rangeMax;
  bool isRangeScan;

  ScanRange(uint64_t min, uint64_t max) : rangeMin(min), rangeMax(max), isRangeScan(true) {}

  explicit ScanRange(uint64_t v) : rangeMin(v), isRangeScan(false) {}

  bool operator==(const ScanRange& rhs) const;

  nebula::storage::cpp2::IndexColumnHint toIndexColumnHint() const;
};

class GeoIndex {
 public:
  explicit GeoIndex(const RegionCoverParams& params, bool pointsOnly = false)
      : rcParams_(params), pointsOnly_(pointsOnly) {}

  // Build geo index for the geography g
  std::vector<uint64_t> indexCells(const Geography& g) const;

  // Query the geo index
  // ST_Intersects(g, x), x is the indexed geography column
  std::vector<ScanRange> intersects(const Geography& g) const;
  // ST_Covers(g, x), x is the indexed geography column
  std::vector<ScanRange> covers(const Geography& g) const;
  // ST_CoveredBy(g, x), x is the indexed geography column
  std::vector<ScanRange> coveredBy(const Geography& g) const;
  // ST_Distance(g, x, distance), x is the indexed geography column
  std::vector<ScanRange> dWithin(const Geography& g, double distance) const;

 private:
  std::vector<ScanRange> intersects(const S2Region& r, bool isPoint = false) const;

  std::vector<S2CellId> coveringCells(const S2Region& r, bool isPoint = false) const;

  std::vector<S2CellId> ancestorCells(const std::vector<S2CellId>& cells) const;

 private:
  RegionCoverParams rcParams_;
  // pointsOnly_ indicates whether the indexed column only has points.
  // For the column Geography(Point), we don't need to build ancestor cells
  bool pointsOnly_{false};
};

}  // namespace geo
}  // namespace nebula

namespace std {

// Inject a customized hash function
template <>
struct hash<S2CellId> {
  std::size_t operator()(const S2CellId& h) const noexcept;
};

}  // namespace std
#endif
