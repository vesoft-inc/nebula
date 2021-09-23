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

struct Geometry {
  virtual ShapeType shape() const = 0;
  virtual ~Geometry() {}
};

struct Point : public Geometry {
  double x, y;
  virtual ShapeType shape() const { return ShapeType::Point; }
  ~Point() override = default;
};

struct LineString : public Geometry {
  std::vector<Point> points;
  virtual ShapeType shape() const { return ShapeType::LineString; }
  ~LineString() override = default;
};

struct Polygon : public Geometry {
  std::vector<LineString> rings;
  virtual ShapeType shape() const { return ShapeType::Polygon; }
  ~Polygon() override = default;
};

}  // namespace nebula
