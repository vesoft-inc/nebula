/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "GeoParams.h"

DEFINE_uint32(min_cell_level, 10, "Minimum cell level for geo. [0, 30]");
DEFINE_validator(min_cell_level, nebula::geo::RegionCoverParams::minCellLevelValidator);

DEFINE_uint32(max_cell_level, 18, "Maximum cell level for geo. [0, 30]");
DEFINE_validator(max_cell_level, nebula::geo::RegionCoverParams::maxCellLevelValidator);

DEFINE_uint32(max_cover_cell_num, 18, "Max cell numbers to cover region for geo.");
