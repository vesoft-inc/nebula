/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <s2/s2point.h>
#include <s2/s2point_region.h>
#include <s2/s2polyline.h>
#include <s2/s2region.h>

#include <algorithm>
#include <typeinfo>
#include <vector>

#include "common/datatypes/Value.h"
#include "common/geo/GeoShape.h"

class S2Polygon;

namespace nebula {

// clang-format off
/*
static const std::unordered_map<ShapeType, S2Region> kShapeTypeToS2Region = {
    {ShapeType::Point, S2PointRegion}, // S2PointRegion is a wrapper of S2Point, and it inherits from the S2Region class
    {ShapeType::LineString, S2Polyline},
    {ShapeType::Polygon, S2Polygon},
};
*/
// clang-format on

// Do not construct a S2 data when constructing Geography. It's expensive.
// We just construct S2 when doing computation.
struct Geography {
  std::string wkb;  // TODO(jie) Maybe store Geometry* here is better

  Geography() = default;
  explicit Geography(const std::string& validWKB) {
    // DCHECK(WKB::isValid(wkb));
    wkb = validWKB;
    LOG(INFO) << "Geography.wkb: " << wkb << ", wkb.size(): " << wkb.size();
  }

  ShapeType shape() const;

  std::string asWKT() const;

  std::string asWKB() const { return wkb; }

  S2Region* asS2() const;

  std::string toString() const { return asWKT(); }

  folly::dynamic toJson() const { return toString(); }

  void clear() { wkb.clear(); }

  void __clear() { clear(); }

  bool operator==(const Geography& rhs) const { return wkb == rhs.wkb; }

  bool operator!=(const Geography& rhs) const { return !(wkb == rhs.wkb); }

  bool operator<(const Geography& rhs) const { return wkb < rhs.wkb; }

  // private:
  //   S2Region* s2RegionFromGeom(const geos::geom::Geometry* geom);
};

inline std::ostream& operator<<(std::ostream& os, const Geography& g) { return os << g.wkb; }

}  // namespace nebula

namespace std {

// Inject a customized hash function
template <>
struct hash<nebula::Geography> {
  std::size_t operator()(const nebula::Geography& h) const noexcept;
};

}  // namespace std
