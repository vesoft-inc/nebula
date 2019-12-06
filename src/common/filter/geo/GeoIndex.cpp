/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Status.h"
#include "filter/geo/GeoIndex.h"
#include "filter/geo/GeoParams.h"
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2polyline.h>

namespace nebula {
namespace geo {
Status GeoIndex::indexCells(const GeoVariant &gv, std::vector<S2CellId> &cells) {
    Status status;
    switch (GeoVariantType(gv.which())) {
        case GeoVariantType::POINT:
            {
                status = indexCellsForPoint(boost::get<Point>(gv), cells);
                break;
            }
        case GeoVariantType::LINESTRING:
        case GeoVariantType::POLYGON:
        default:
            Status::Error("Not support yet.");
    }
    return status;
}

Status GeoIndex::indexCellsForPoint(const Point &p, std::vector<S2CellId> &cells) {
    const auto sll = S2LatLng::FromDegrees(p.x(), p.y());
    const S2CellId s2Id(sll);

    for (auto level = rcParams_.minCellLevel_; level <= rcParams_.maxCellLevel_; ++level) {
        cells.emplace_back(s2Id.parent(level));
    }

    return Status::OK();
}

Status GeoIndex::indexCellsForLineString(const LineString &line, std::vector<S2CellId> &cells) {
    S2Polyline s2Line;
    std::vector<S2LatLng> slls;
    for (auto &p : line) {
        auto sll = S2LatLng::FromDegrees(p.x(), p.y());
        slls.emplace_back(std::move(sll));
    }
    s2Line.Init(slls);

    S2RegionCoverer rc(rcParams_.regionCovererOpts());
    auto cover = rc.GetCovering(s2Line);

    for (auto &cell : cover) {
        for (auto level = rcParams_.minCellLevel_; level <= rcParams_.maxCellLevel_; ++level) {
            cells.emplace_back(cell.parent(level));
        }
    }

    return Status::OK();
}

Status GeoIndex::indexCellsForPolygon(const Polygon &polygon, std::vector<S2CellId> &cells) {
    UNUSED(polygon);
    UNUSED(cells);
    // TODO:
    // 1. No ajacent in a loop
    // 2. No intersect in loops
    return Status::OK();
}
}  // namespace geo
}  // namespace nebula
