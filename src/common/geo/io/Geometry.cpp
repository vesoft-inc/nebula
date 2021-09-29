/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/io/Geometry.h"

#include "common/geo/GeoUtils.h"

namespace nebula {

void LineString::normalize() {
  GeoUtils::removeAdjacentDuplicateCoordinates(coordList);
  for (auto& coord : coordList) {
    coord.normalize();
  }
}

void Polygon::normalize() {
  for (auto& coordList : coordListList) {
    GeoUtils::removeAdjacentDuplicateCoordinates(coordList);
    for (auto& coord : coordList) {
      coord.normalize();
    }
  }
}

bool Geometry::isValid() const {
  switch (shape()) {
    case GeoShape::POINT:
      return point().isValid();
    case GeoShape::LINESTRING:
      return lineString().isValid();
    case GeoShape::POLYGON:
      return polygon().isValid();
    case GeoShape::UNKNOWN:
    default:
      LOG(ERROR)
          << "Geometry shapes other than Point/LineString/Polygon are not currently supported";
      return false;
  }
}

void Geometry::normalize() {
  switch (shape()) {
    case GeoShape::POINT:
      std::get<Point>(geom).normalize();
      return;
    case GeoShape::LINESTRING:
      std::get<LineString>(geom).normalize();
      return;
    case GeoShape::POLYGON:
      std::get<Polygon>(geom).normalize();
      return;
    case GeoShape::UNKNOWN:
    default:
      LOG(ERROR)
          << "Geometry shapes other than Point/LineString/Polygon are not currently supported";
  }
}

bool operator==(const Geometry& lhs, const Geometry& rhs) {
  auto lhsShape = lhs.shape();
  auto rhsShape = rhs.shape();
  if (lhsShape != rhsShape) {
    return false;
  }

  switch (lhsShape) {
    case GeoShape::POINT: {
      return lhs.point() == rhs.point();
    }
    case GeoShape::LINESTRING: {
      return lhs.lineString() == rhs.lineString();
    }
    case GeoShape::POLYGON: {
      return lhs.polygon() == rhs.polygon();
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return false;
    }
  }
}

bool operator!=(const Geometry& lhs, const Geometry& rhs) { return !(lhs == rhs); }

}  // namespace nebula
