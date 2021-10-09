/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <s2/s2region_coverer.h>

#include "common/datatypes/Geography.h"

namespace nebula {

uint64_t s2CellIdFromPoint(const Geography& a, int level = 30);

std::vector<uint64_t> s2CoveringCellIds(const Geography& a,
                                        int minLevel = 0,
                                        int maxLevel = 30,
                                        int maxCells = 8,
                                        double bufferInMeters = 0.0);

std::vector<uint64_t> coveringCellIds(const S2Region& r, const S2RegionCoverer::Options& opts);

}  // namespace nebula
