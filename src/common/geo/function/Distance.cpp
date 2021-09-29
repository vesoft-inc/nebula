/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/function/Distance.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2closest_edge_query.h>
#include <s2/s2earth.h>
#include <s2/s2polygon.h>

namespace nebula {

// Find the closest distance of a and b
double distance(const Geography& a, const Geography& b) {
  auto aRegion = a.asS2();
  auto bRegion = b.asS2();
  if (!aRegion || !bRegion) {
    LOG(INFO) << "distance(), asS2() failed.";
    return -1.0;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      const S2Point& aPoint = static_cast<S2PointRegion*>(aRegion.get())->point();
      switch (b.shape()) {
        case GeoShape::POINT: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion.get())->point();
          return S2Earth::GetDistanceMeters(aPoint, bPoint);
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          return distanceOfS2PolylineWithS2Point(bLine, aPoint);
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          return distanceOfS2PolygonWithS2Point(bPolygon, aPoint);
        }
        case GeoShape::UNKNOWN:
        default: {
          LOG(ERROR)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return false;
        }
      }
    }
    case GeoShape::LINESTRING: {
      S2Polyline* aLine = static_cast<S2Polyline*>(aRegion.get());
      switch (b.shape()) {
        case GeoShape::POINT: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion.get())->point();
          return distanceOfS2PolylineWithS2Point(aLine, bPoint);
        }
        case GeoShape::LINESTRING: {
          const S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          MutableS2ShapeIndex aIndex, bIndex;
          aIndex.Add(std::make_unique<S2Polyline::Shape>(aLine));
          bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
          S2ClosestEdgeQuery query(&aIndex);
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bIndex);
          return S2Earth::ToMeters(query.GetDistance(&target));
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          return distanceOfS2PolygonWithS2Polyline(bPolygon, aLine);
        }
        case GeoShape::UNKNOWN:
        default: {
          LOG(ERROR)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return false;
        }
      }
    }
    case GeoShape::POLYGON: {
      S2Polygon* aPolygon = static_cast<S2Polygon*>(aRegion.get());
      switch (b.shape()) {
        case GeoShape::POINT: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion.get())->point();
          return distanceOfS2PolygonWithS2Point(aPolygon, bPoint);
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          return distanceOfS2PolygonWithS2Polyline(aPolygon, bLine);
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          S2ClosestEdgeQuery query(&aPolygon->index());
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bPolygon->index());
          return S2Earth::ToMeters(query.GetDistance(&target));
        }
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

double distanceOfS2PolylineWithS2Point(const S2Polyline* aLine, const S2Point& bPoint) {
  int tmp;
  S2Point cloestPointOnLine = aLine->Project(bPoint, &tmp);
  return S2Earth::GetDistanceMeters(cloestPointOnLine, bPoint);
}

double distanceOfS2PolygonWithS2Polyline(const S2Polygon* aPolygon, const S2Polyline* bLine) {
  S2ClosestEdgeQuery query(&aPolygon->index());
  MutableS2ShapeIndex bIndex;
  bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
  S2ClosestEdgeQuery::ShapeIndexTarget target(&bIndex);
  return S2Earth::ToMeters(query.GetDistance(&target));
}

double distanceOfS2PolygonWithS2Point(const S2Polygon* aPolygon, const S2Point& bPoint) {
  return S2Earth::ToMeters(aPolygon->GetDistance(bPoint));
}

}  // namespace nebula
