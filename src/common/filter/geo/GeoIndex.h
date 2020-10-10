/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FILTER_GEO_GEOINDEX_H_
#define COMMON_FILTER_GEO_GEOINDEX_H_

#include "base/Base.h"
#include "base/Status.h"
#include "filter/geo/GeoParams.h"
#include <s2/s2cell_id.h>

namespace nebula {
namespace geo {
class GeoIndex final {
public:
    Status indexCells(const GeoVariant &gv, std::vector<S2CellId> &cells);

    Status indexCellsForPoint(const Point &p, std::vector<S2CellId> &cells);

    Status indexCellsForLineString(const LineString &line, std::vector<S2CellId> &cells);

    Status indexCellsForPolygon(const Polygon &polygon, std::vector<S2CellId> &cells);
private:
    RegionCoverParams    rcParams_;
};
}  // namespace geo
}  // namespace nebula
#endif
