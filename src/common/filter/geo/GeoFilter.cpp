/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Status.h"
#include "filter/geo/GeoFilter.h"
#include "filter/geo/GeoParams.h"
#include "filter/Expressions.h"
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2cap.h>
#include <s2/s2region_coverer.h>

namespace nebula {
namespace geo {
Status GeoFilter::near(const std::vector<VariantType> &args) {
    auto predicate = args[0];

    std::string pointWkt = "POINT";
    pointWkt.append(boost::get<std::string>(args[1]));
    PointType loc;
    boost::geometry::read_wkt(pointWkt, loc);

    auto dist = Expression::toDouble(args[2]);
    if (dist < 0) {
        return Status::Error("Distance should be a positive number.");
    }

    const auto sll = S2LatLng::FromDegrees(loc.x(), loc.y());
    S1Angle radius = S1Angle::Radians(dist / kEarthRadiusMeters);
    S2Cap cap(sll.ToPoint(), std::move(radius));

    RegionCoverParams rcParams;
    S2RegionCoverer rc(rcParams.regionCovererOpts());
    auto cover = rc.GetCovering(cap);

    for (auto cell : cover) {
        LOG(INFO) << cell.id() << " " << cell.level();
    }
    return Status::Error("To be implemented.");
}
}  // namespace geo
}  // namespace nebula
