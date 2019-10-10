/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FILTER_GEO_GEOPARAMS_H_
#define COMMON_FILTER_GEO_GEOPARAMS_H_

#include "base/Base.h"
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/linestring.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <s2/s2region_coverer.h>

namespace nebula {
namespace geo {
using PointType = boost::geometry::model::d2::point_xy<double>;

using GeoVariant = boost::variant<
    PointType,
    boost::geometry::model::linestring<PointType>,
    boost::geometry::model::polygon<PointType>
    >;

enum class GeoVariantType : uint8_t {
    POINT       = 0,
    LINESTRING  = 1,
    POLYGON     = 2,
};

constexpr char kWktPointPrefix[] = "POINT";
constexpr char kWktLinestringPrefix[] = "LINESTRING";
constexpr char kWktPolygonPrefix[] = "POLYGON";

constexpr double kEarthRadiusMeters = (6371.000 * 1000);

struct RegionCoverParams {
public:
    S2RegionCoverer::Options regionCovererOpts() const {
        S2RegionCoverer::Options opts;
        opts.set_min_level(minCellLevel_);
        opts.set_max_level(maxCellLevel_);
        opts.set_max_cells(maxCoverCellNum_);
        return opts;
    }

public:
    // minCellLevel indicates the min level that used by covering. [0, 30]
    int minCellLevel_ = 5;
    // maxCellLevel_ indicates the max level that used by covering. [0, 30]
    int maxCellLevel_ = 24;
    // maxCoverCellNum_ indicates the max number cells that covering the regin.
    int maxCoverCellNum_ = 18;
};

}  // namespace geo
}  // namespace nebula
#endif
