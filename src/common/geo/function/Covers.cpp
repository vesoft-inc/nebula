/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/function/Covers.h"

#include <s2/s2cell.h>
#include <s2/s2polygon.h>

namespace nebula {

// Covers returns whether geography b covers geography b.
// If no point in b lies exterior of b, a covers b.
// http://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
bool covers(const Geography& a, const Geography& b) {
  auto* aRegion = a.asS2();
  auto* bRegion = b.asS2();

  switch (a.shape()) {
    case ShapeType::Point: {
      switch (b.shape()) {
        case ShapeType::Point:
          return static_cast<S2PointRegion*>(aRegion)->Contains(
              static_cast<S2PointRegion*>(bRegion)->point());
        case ShapeType::LineString:
          return false;
        case ShapeType::Polygon:
          return false;
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
    case ShapeType::LineString: {
      S2Polyline* aLine = static_cast<S2Polyline*>(aRegion);
      switch (b.shape()) {
        case ShapeType::Point:
          return aLine->MayIntersect(S2Cell(static_cast<S2PointRegion*>(bRegion)->point()));
        case ShapeType::LineString:
          return aLine->NearlyCovers(*static_cast<S2Polyline*>(bRegion), S1Angle::Radians(1e-15));
        case ShapeType::Polygon:
          return false;
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
    case ShapeType::Polygon: {
      S2Polygon* aPolygon = static_cast<S2Polygon*>(aRegion);
      switch (b.shape()) {
        case ShapeType::Point:
          return aPolygon->Contains(static_cast<S2PointRegion*>(bRegion)->point());
        case ShapeType::LineString:
          return aPolygon->Contains(*static_cast<S2Polyline*>(bRegion));
        case ShapeType::Polygon:
          return aPolygon->Contains(static_cast<S2Polygon*>(bRegion));
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
  }

  return false;
}

bool coveredBy(const Geography& a, const Geography& b) { return covers(b, a); }

}  // namespace nebula
