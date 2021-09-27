/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <algorithm>
#include <typeinfo>
#include <vector>

#include "common/geo/GeoShape.h"

namespace nebula {

// These Geometry structs are just for parsing wkt/wkb
struct Coordinate {
  double x, y;

  Coordinate(double lng, double lat) : x(lng), y(lat) {}

  // TODO(jie) compare double correctly
  bool operator==(const Coordinate &rhs) const { return x == rhs.x && y == rhs.y; }
  bool operator!=(const Coordinate &rhs) const { return !(*this == rhs); }

  static bool isValidLng(double lng) { return std::abs(lng) <= 180; }

  static bool isValidLat(double lat) { return std::abs(lat) <= 90; }
};

struct Geometry {
  virtual ~Geometry() {}
  virtual GeoShape shape() const = 0;
};

struct Point : public Geometry {
  Coordinate coord;

  explicit Point(const Coordinate &v) : coord(v) {}
  explicit Point(Coordinate &&v) : coord(std::move(v)) {}
  ~Point() override = default;

  GeoShape shape() const override { return GeoShape::POINT; }
};

struct LineString : public Geometry {
  std::vector<Coordinate> coordList;

  explicit LineString(const std::vector<Coordinate> &v) : coordList(v) {}
  explicit LineString(std::vector<Coordinate> &&v) : coordList(std::move(v)) {}
  ~LineString() override = default;

  GeoShape shape() const override { return GeoShape::LINESTRING; }
  uint32_t numCoords() const { return coordList.size(); }
};

struct Polygon : public Geometry {
  std::vector<std::vector<Coordinate>> coordListList;

  explicit Polygon(const std::vector<std::vector<Coordinate>> &v) : coordListList(v) {}
  explicit Polygon(std::vector<std::vector<Coordinate>> &&v) : coordListList(std::move(v)) {}
  ~Polygon() override = default;

  GeoShape shape() const override { return GeoShape::POLYGON; }
  uint32_t numRings() const { return coordListList.size(); }
  uint32_t numInteriorRing() const { return numRings() - 1; }
};

}  // namespace nebula
