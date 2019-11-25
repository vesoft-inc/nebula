/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/StatusOr.h"
#include "filter/geo/GeoFilter.h"
#include "filter/geo/GeoParams.h"
#include "filter/Expressions.h"
#include "filter/geo/GeoIndex.h"
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2cap.h>
#include <s2/s2region_coverer.h>

namespace nebula {
namespace geo {
StatusOr<std::string> GeoFilter::near(const std::vector<VariantType> &args) {
    if (args.size() < 2) {
        return Status::Error("Function `near' should be given 2 args.");
    }

    // `predicate' is a reserved args,
    // we might use it when we support geo with index.
    // auto predicate = args[0];

    std::string pointWkt = kWktPointPrefix;
    pointWkt.append(boost::get<std::string>(args[0]));
    Point loc;
    boost::geometry::read_wkt(pointWkt, loc);

    auto dist = Expression::toDouble(args[1]);
    if (dist < 0) {
        return Status::Error("Distance should be a positive number.");
    }

    const auto sll = S2LatLng::FromDegrees(loc.x(), loc.y());
    S1Angle radius = S1Angle::Radians(dist / kEarthRadiusMeters);
    S2Cap cap(sll.ToPoint(), std::move(radius));

    RegionCoverParams rcParams;
    S2RegionCoverer rc(rcParams.regionCovererOpts());
    auto cover = rc.GetCovering(cap);

    std::string s;
    for (auto &cellId : cover) {
        folly::toAppend(cellId.id(), &s);
        folly::toAppend(",", &s);
    }
    s.pop_back();
    return s;
}
}  // namespace geo
}  // namespace nebula
