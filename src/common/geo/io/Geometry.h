/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <algorithm>
#include <typeinfo>
#include <variant>
#include <vector>

#include "common/base/Base.h"
#include "common/geo/GeoShape.h"

namespace nebula {

// These Geometry structs are just for parsing wkt/wkb
struct Coordinate {
  double x, y;

  Coordinate() = default;
  Coordinate(double lng, double lat) : x(lng), y(lat) {}

  // TODO(jie) compare double correctly
  bool operator==(const Coordinate& rhs) const { return x == rhs.x && y == rhs.y; }
  bool operator!=(const Coordinate& rhs) const { return !(*this == rhs); }

  static bool isValidLng(double lng) { return std::abs(lng) <= 180; }

  static bool isValidLat(double lat) { return std::abs(lat) <= 90; }
};

struct Point {
  Coordinate coord;

  explicit Point(const Coordinate& v) : coord(v) {}
  explicit Point(Coordinate&& v) : coord(std::move(v)) {}
  ~Point() {}
};

struct LineString {
  std::vector<Coordinate> coordList;

  explicit LineString(const std::vector<Coordinate>& v) : coordList(v) {}
  explicit LineString(std::vector<Coordinate>&& v) : coordList(std::move(v)) {}
  ~LineString() {}

  uint32_t numCoords() const { return coordList.size(); }
};

struct Polygon {
  std::vector<std::vector<Coordinate>> coordListList;

  explicit Polygon(const std::vector<std::vector<Coordinate>>& v) : coordListList(v) {}
  explicit Polygon(std::vector<std::vector<Coordinate>>&& v) : coordListList(std::move(v)) {}
  ~Polygon() {}

  uint32_t numRings() const { return coordListList.size(); }
  uint32_t numInteriorRing() const { return numRings() - 1; }
};

struct Geometry {
  std::variant<Point, LineString, Polygon> geom;

  Geometry(const Point& v) : geom(v) {}             // NOLINT
  Geometry(Point&& v) : geom(std::move(v)) {}       // NOLINT
  Geometry(const LineString& v) : geom(v) {}        // NOLINT
  Geometry(LineString&& v) : geom(std::move(v)) {}  // NOLINT
  Geometry(const Polygon& v) : geom(v) {}           // NOLINT
  Geometry(Polygon&& v) : geom(std::move(v)) {}     // NOLINT

  GeoShape shape() const {
    switch (geom.index()) {
      case 0:
        return GeoShape::POINT;
      case 1:
        return GeoShape::LINESTRING;
      case 2:
        return GeoShape::POLYGON;
      default:
        return GeoShape::UNKNOWN;
    }
  }
  const Point& point() const {
    CHECK(std::holds_alternative<Point>(geom));
    return std::get<Point>(geom);
  }

  const LineString& lineString() const {
    CHECK(std::holds_alternative<LineString>(geom));
    return std::get<LineString>(geom);
  }

  const Polygon& polygon() const {
    CHECK(std::holds_alternative<Polygon>(geom));
    return std::get<Polygon>(geom);
  }
};

}  // namespace nebula
