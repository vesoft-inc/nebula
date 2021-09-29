/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <s2/s2region_coverer.h>

#include "common/datatypes/Geography.h"

namespace nebula {

uint64_t S2CellIdFromPoint(const Geography& a, int level);

std::vector<uint64_t> S2CoveringCellIds(
    const Geography& a, int minLevel, int maxLevel, int maxCells, double bufferInMeters);

std::vector<uint64_t> coveringCellIds(const S2Region& r, const S2RegionCoverer::Options& opts);

}  // namespace nebula
