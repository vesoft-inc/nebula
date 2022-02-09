/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_GEOGRAPHY_H
#define COMMON_DATATYPES_GEOGRAPHY_H

#include <s2/s2point.h>
#include <s2/s2point_region.h>
#include <s2/s2polyline.h>
#include <s2/s2region.h>

#include <variant>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Value.h"

// Do not include <s2/s2polygon.h> here, it will indirectly includes a header file which defines a
// enum `BEGIN`(not enum class). While Geography.h is indirectly included by parser.yy, which has a
// macro named `BEGIN`. So they will be conflicted.

class S2Polygon;

namespace nebula {

enum class GeoShape : uint32_t {
  UNKNOWN = 0,  // illegal
  POINT = 1,
  LINESTRING = 2,
  POLYGON = 3,
};

std::ostream& operator<<(std::ostream& os, const GeoShape& shape);

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

struct Coordinate {
  double x, y;

  Coordinate() = default;
  Coordinate(double lng, double lat) : x(lng), y(lat) {}

  void normalize();
  bool isValid() const;

  void clear() {
    x = 0.0;
    y = 0.0;
  }
  void __clear() {
    clear();
  }

  bool operator==(const Coordinate& rhs) const {
    return std::abs(x - rhs.x) < kEpsilon && std::abs(y - rhs.y) < kEpsilon;
  }
  bool operator!=(const Coordinate& rhs) const {
    return !(*this == rhs);
  }
  bool operator<(const Coordinate& rhs) const {
    if (x != rhs.x) {
      return x < rhs.x;
    }
    if (y != rhs.y) {
      return y < rhs.y;
    }
    return false;
  }
};

struct Point {
  Coordinate coord;

  Point() = default;
  explicit Point(const Coordinate& v) : coord(v) {}
  explicit Point(Coordinate&& v) : coord(std::move(v)) {}

  void normalize();
  bool isValid() const;

  void clear() {
    coord.clear();
  }
  void __clear() {
    clear();
  }

  bool operator==(const Point& rhs) const {
    return coord == rhs.coord;
  }
  bool operator<(const Point& rhs) const {
    return coord < rhs.coord;
  }
};

struct LineString {
  std::vector<Coordinate> coordList;

  LineString() = default;
  explicit LineString(const std::vector<Coordinate>& v) : coordList(v) {}
  explicit LineString(std::vector<Coordinate>&& v) : coordList(std::move(v)) {}

  uint32_t numCoord() const {
    return coordList.size();
  }

  void normalize();
  bool isValid() const;

  void clear() {
    coordList.clear();
  }
  void __clear() {
    clear();
  }

  bool operator==(const LineString& rhs) const {
    return coordList == rhs.coordList;
  }
  bool operator<(const LineString& rhs) const {
    return coordList < rhs.coordList;
  }
};

struct Polygon {
  std::vector<std::vector<Coordinate>> coordListList;

  Polygon() = default;
  explicit Polygon(const std::vector<std::vector<Coordinate>>& v) : coordListList(v) {}
  explicit Polygon(std::vector<std::vector<Coordinate>>&& v) : coordListList(std::move(v)) {}

  uint32_t numCoordList() const {
    return coordListList.size();
  }

  void normalize();
  bool isValid() const;

  void clear() {
    coordListList.clear();
  }
  void __clear() {
    clear();
  }

  bool operator==(const Polygon& rhs) const {
    return coordListList == rhs.coordListList;
  }
  bool operator<(const Polygon& rhs) const {
    return coordListList < rhs.coordListList;
  }
};

struct Geography {
  std::variant<Point, LineString, Polygon> geo_;

  // Factory method
  static StatusOr<Geography> fromWKT(const std::string& wkt,
                                     bool needNormalize = false,
                                     bool verifyValidity = false);

  static StatusOr<Geography> fromWKB(const std::string& wkb,
                                     bool needNormalize = false,
                                     bool verifyValidity = false);

  Geography() = default;
  Geography(const Point& v) : geo_(v) {}             // NOLINT
  Geography(Point&& v) : geo_(std::move(v)) {}       // NOLINT
  Geography(const LineString& v) : geo_(v) {}        // NOLINT
  Geography(LineString&& v) : geo_(std::move(v)) {}  // NOLINT
  Geography(const Polygon& v) : geo_(v) {}           // NOLINT
  Geography(Polygon&& v) : geo_(std::move(v)) {}     // NOLINT

  GeoShape shape() const;

  const Point& point() const;
  const LineString& lineString() const;
  const Polygon& polygon() const;

  Point& mutablePoint();
  LineString& mutableLineString();
  Polygon& mutablePolygon();

  void normalize();
  bool isValid() const;

  Point centroid() const;

  std::string asWKT() const;

  std::string asWKB() const;

  std::string asWKBHex() const;

  std::unique_ptr<S2Region> asS2() const;

  std::string toString() const {
    return asWKT();
  }

  folly::dynamic toJson() const {
    return toString();
  }

  void clear() {
    geo_.~variant();
  }

  void __clear() {
    clear();
  }

  bool operator==(const Geography& rhs) const;
  bool operator<(const Geography& rhs) const;
};

inline std::ostream& operator<<(std::ostream& os, const Geography& g) {
  return os << g.toString();
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
template <>
struct hash<nebula::Geography> {
  std::size_t operator()(const nebula::Geography& h) const noexcept;
};

}  // namespace std
#endif
