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

  void normalize() {
    // Reduce the x(longitude) to the range [-180, 180] degrees
    x = std::remainder(x, 360.0);

    // Reduce the y(latitude) to the range [-90, 90] degrees
    double tmp = remainder(y, 360.0);
    if (tmp > 90.0) {
      y = 180.0 - tmp;
    }
    if (tmp < -90.0) {
      y = -180.0 - tmp;
    }
  }

  // TODO(jie) compare double correctly
  bool operator==(const Coordinate& rhs) const { return x == rhs.x && y == rhs.y; }
  bool operator!=(const Coordinate& rhs) const { return !(*this == rhs); }
};

struct Point {
  Coordinate coord;

  explicit Point(const Coordinate& v) : coord(v) {}
  explicit Point(Coordinate&& v) : coord(std::move(v)) {}
  ~Point() = default;

  bool isValid() const { return true; }

  void normalize() { return coord.normalize(); }

  bool operator==(const Point& rhs) const { return coord == rhs.coord; }
};

struct LineString {
  std::vector<Coordinate> coordList;

  explicit LineString(const std::vector<Coordinate>& v) : coordList(v) {}
  explicit LineString(std::vector<Coordinate>&& v) : coordList(std::move(v)) {}
  ~LineString() = default;

  uint32_t numCoord() const { return coordList.size(); }

  bool isValid() const {
    // LineString must have at least 2 coordinates;
    return coordList.size() >= 2;
  }

  void normalize();

  bool operator==(const LineString& rhs) const { return coordList == rhs.coordList; }
};

struct Polygon {
  std::vector<std::vector<Coordinate>> coordListList;

  explicit Polygon(const std::vector<std::vector<Coordinate>>& v) : coordListList(v) {}
  explicit Polygon(std::vector<std::vector<Coordinate>>&& v) : coordListList(std::move(v)) {}
  ~Polygon() = default;

  uint32_t numCoordList() const { return coordListList.size(); }

  bool isValid() const {
    for (const auto& coordList : coordListList) {
      // Polygon's LinearRing must have at least 4 coordinates
      if (coordList.size() < 4) {
        return false;
      }
      // Polygon's LinearRing must be closed
      if (coordList.front() != coordList.back()) {
        return false;
      }
    }
    return true;
  }

  void normalize();

  bool operator==(const Polygon& rhs) const { return coordListList == rhs.coordListList; }
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

  bool isValid() const;

  void normalize();
};

bool operator==(const Geometry& lhs, const Geometry& rhs);

bool operator!=(const Geometry& lhs, const Geometry& rhs);

}  // namespace nebula
