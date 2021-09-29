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

#include "common/base/StatusOr.h"
#include "common/datatypes/Value.h"
#include "common/geo/io/Geometry.h"

// Do not include <s2/s2polygon.h> here, it will indirectly includes a header file which defines a
// enum `BEGIN`(not enum class). While Geography.h is indirectly included by parser.yy, which has a
// macro named `BEGIN`. So they will be conflicted.

class S2Polygon;

namespace nebula {

// clang-format off
/*
static const std::unordered_map<GeoShape, S2Region> kShapeTypeToS2Region = {
    // S2PointRegion is a wrapper of S2Point, and it inherits from the S2Region class while S2Point doesn't.
    {GeoShape::POINT, S2PointRegion},
    {GeoShape::LINESTRING, S2Polyline},
    {GeoShape::POLYGON, S2Polygon},
};
*/
// clang-format on

// Do not construct a S2 object when constructing Geography. It's expensive.
// We just construct S2 when doing computation.
struct Geography {
  std::string wkb;  // TODO(jie) Is it better to store Geometry* or S2Region* here?

  Geography() = default;

  static StatusOr<Geography> fromWKT(const std::string& wkt);

  GeoShape shape() const;

  std::unique_ptr<std::string> asWKT() const;

  std::unique_ptr<std::string> asWKBHex() const;

  std::unique_ptr<S2Region> asS2() const;

  std::string toString() const { return wkb; }

  folly::dynamic toJson() const { return toString(); }

  void clear() { wkb.clear(); }

  void __clear() { clear(); }

  bool operator==(const Geography& rhs) const { return wkb == rhs.wkb; }

  bool operator!=(const Geography& rhs) const { return !(wkb == rhs.wkb); }

  bool operator<(const Geography& rhs) const { return wkb < rhs.wkb; }

 private:
  explicit Geography(const std::string& bytes) {
    // TODO(jie): Must ensure the bytes is valid
    wkb = bytes;
  }
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
