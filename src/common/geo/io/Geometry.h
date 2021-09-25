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
};

struct Geometry {
  virtual ShapeType shape() const = 0;
  virtual ~Geometry() {}
};

struct Point : public Geometry {
  Coordinate coord;
  ShapeType shape() const override { return ShapeType::Point; }
  explicit Point(const Coordinate &v) : coord(v) {}
  explicit Point(Coordinate &&v) : coord(std::move(v)) {}
  ~Point() override = default;
};

struct LineString : public Geometry {
  std::vector<Coordinate> coordList;
  ShapeType shape() const override { return ShapeType::LineString; }
  explicit LineString(const std::vector<Coordinate> &v) : coordList(v) {}
  explicit LineString(std::vector<Coordinate> &&v) : coordList(std::move(v)) {}
  ~LineString() override = default;
};

struct Polygon : public Geometry {
  std::vector<std::vector<Coordinate>> coordListList;
  ShapeType shape() const override { return ShapeType::Polygon; }
  explicit Polygon(const std::vector<std::vector<Coordinate>> &v) : coordListList(v) {}
  explicit Polygon(std::vector<std::vector<Coordinate>> &&v) : coordListList(std::move(v)) {}
  ~Polygon() override = default;
};

}  // namespace nebula
