/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

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

struct RegionCoverParams {
  int minCellLevel_ = 4;
  int maxCellLevel_ = 23;  // 30?
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

  explicit ScanRange(uint64_t min) : rangeMin(min), isRangeScan(false) {}

  storage::cpp2::IndexColumnHint toIndexColumnHint();
};

class GeographyIndex {
 public:
  explicit GeographyIndex(const RegionCoverParams& params, bool pointsOnly = false)
      : rcParams_(params), pointsOnly_(pointsOnly) {}

  // build index
  std::vector<uint64_t> indexCells(const Geography& g) const noexcept;

  // query index
  std::vector<ScanRange> covers(const Geography& g) const noexcept;

  std::vector<ScanRange> coveredBy(const Geography& g) const noexcept;

  std::vector<ScanRange> intersects(const Geography& g) const noexcept;

  std::vector<ScanRange> dWithin(const Geography& g, double distance) const noexcept;

 private:
  std::vector<S2CellId> coveringCells(const Geography& g) const noexcept;
  std::vector<S2CellId> coveringCells(const S2Region& r) const noexcept;

  std::vector<S2CellId> ancestorCells(const std::vector<S2CellId>& cells) const noexcept;

  std::vector<ScanRange> scanRange(const std::vector<S2CellId>& cells) const noexcept;

 private:
  RegionCoverParams rcParams_;
  bool pointsOnly_{
      false};  // For the column Geography(Point), we don't need to build ancestor cells
};

}  // namespace nebula

namespace std {

// Inject a customized hash function
template <>
struct hash<S2CellId> {
  std::size_t operator()(const S2CellId& h) const noexcept;
};

}  // namespace std
