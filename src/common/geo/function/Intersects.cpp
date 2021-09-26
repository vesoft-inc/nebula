/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/function/Intersects.h"

#include <s2/s2cell.h>
#include <s2/s2polygon.h>

namespace nebula {

// Intersects returns whether geography b intersects geography b.
// If any point in the set that comprises A is also a member of the set of points that make up B,
// they intersects;
bool intersects(const Geography& a, const Geography& b) {
  auto aRegion = a.asS2().get();
  auto bRegion = b.asS2().get();
  // TODO(jie): Better to ensure the Geography value is valid to build s2 when constructing.
  if (!aRegion || !bRegion) {
    LOG(INFO) << "intersects(), asS2() failed.";
    return false;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      switch (b.shape()) {
        case GeoShape::POINT:
          return static_cast<S2PointRegion*>(aRegion)->MayIntersect(
              S2Cell(static_cast<S2PointRegion*>(bRegion)->point()));
        case GeoShape::LINESTRING:
          return static_cast<S2Polyline*>(bRegion)->MayIntersect(
              S2Cell(static_cast<S2PointRegion*>(aRegion)->point()));
        case GeoShape::POLYGON:
          return static_cast<S2Polygon*>(bRegion)->MayIntersect(
              S2Cell(static_cast<S2PointRegion*>(aRegion)->point()));
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
    case GeoShape::LINESTRING: {
      switch (b.shape()) {
        case GeoShape::POINT:
          return static_cast<S2Polyline*>(aRegion)->MayIntersect(
              S2Cell(static_cast<S2PointRegion*>(bRegion)->point()));
        case GeoShape::LINESTRING:
          return static_cast<S2Polyline*>(aRegion)->Intersects(static_cast<S2Polyline*>(bRegion));
        case GeoShape::POLYGON:
          return static_cast<S2Polygon*>(bRegion)->Intersects(*static_cast<S2Polyline*>(aRegion));
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
    case GeoShape::POLYGON: {
      switch (b.shape()) {
        case GeoShape::POINT:
          return static_cast<S2Polygon*>(aRegion)->MayIntersect(
              S2Cell(static_cast<S2PointRegion*>(bRegion)->point()));
        case GeoShape::LINESTRING:
          return static_cast<S2Polygon*>(aRegion)->Intersects(*static_cast<S2Polyline*>(bRegion));
        case GeoShape::POLYGON:
          return static_cast<S2Polygon*>(aRegion)->Intersects(static_cast<S2Polygon*>(bRegion));
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
  }

  return false;
}

}  // namespace nebula
