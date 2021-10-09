/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/function/Covers.h"

#include <s2/s2cell.h>
#include <s2/s2polygon.h>

#include "common/geo/GeoShape.h"

namespace nebula {

// Returns true if no point in b lies exterior of b.
// The difference between ST_Covers, ST_Contains and ST_ContainsProperly, see
// http://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
bool covers(const Geography& a, const Geography& b) {
  auto aRegion = a.asS2();
  auto bRegion = b.asS2();
  if (!aRegion || !bRegion) {
    return false;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      switch (b.shape()) {
        case GeoShape::POINT:
          return static_cast<S2PointRegion*>(aRegion.get())
              ->Contains(static_cast<S2PointRegion*>(bRegion.get())->point());
        case GeoShape::LINESTRING:
        case GeoShape::POLYGON:
          return false;
        case GeoShape::UNKNOWN:
        default:
          LOG(ERROR)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return false;
      }
    }
    case GeoShape::LINESTRING: {
      S2Polyline* aLine = static_cast<S2Polyline*>(aRegion.get());
      switch (b.shape()) {
        case GeoShape::POINT:
          return aLine->MayIntersect(S2Cell(static_cast<S2PointRegion*>(bRegion.get())->point()));
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          if (aLine->NearlyCovers(*bLine, S1Angle::Radians(1e-15))) {
            return true;
          }
          // LineString should covers its reverse
          aLine->Reverse();
          return aLine->NearlyCovers(*bLine, S1Angle::Radians(1e-15));
        }
        case GeoShape::POLYGON:
          return false;
        case GeoShape::UNKNOWN:
        default:
          LOG(ERROR)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return false;
      }
    }
    case GeoShape::POLYGON: {
      S2Polygon* aPolygon = static_cast<S2Polygon*>(aRegion.get());
      switch (b.shape()) {
        case GeoShape::POINT:
          return aPolygon->Contains(static_cast<S2PointRegion*>(bRegion.get())->point());
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          if (aPolygon->Contains(*bLine)) {
            return true;
          }
          bLine->Reverse();
          return aPolygon->Contains(*bLine);
        }
        case GeoShape::POLYGON:
          return aPolygon->Contains(static_cast<S2Polygon*>(bRegion.get()));
        case GeoShape::UNKNOWN:
        default: {
          LOG(ERROR)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return false;
        }
      }
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return false;
    }
  }

  return false;
}

bool coveredBy(const Geography& a, const Geography& b) { return covers(b, a); }

}  // namespace nebula
