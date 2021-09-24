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
};

struct Geometry {
  virtual ShapeType shape() const = 0;
  virtual ~Geometry() {}
};

struct Point : public Geometry {
  Coordinate coord;
  ShapeType shape() const override { return ShapeType::Point; }
  ~Point() override = default;
};

struct LineString : public Geometry {
  std::vector<Coordinate> coordList;
  ShapeType shape() const override { return ShapeType::LineString; }
  ~LineString() override = default;
};

struct Polygon : public Geometry {
  std::vector<std::vector<Coordinate>> coordListList;
  ShapeType shape() const override { return ShapeType::Polygon; }
  ~Polygon() override = default;
};

}  // namespace nebula
