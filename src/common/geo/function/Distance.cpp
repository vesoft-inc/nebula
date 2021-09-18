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

  switch (a.shape()) {
    case ShapeType::Point: {
      const S2Point& aPoint = static_cast<S2PointRegion*>(aRegion)->point();
      switch (b.shape()) {
        case ShapeType::Point: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion)->point();
          return S2Earth::GetDistanceMeters(aPoint, bPoint);
        }
        case ShapeType::LineString: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion);
          return distanceOfS2PolylineWithS2Point(bLine, aPoint);
        }
        case ShapeType::Polygon: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion);
          return distanceOfS2PolygonWithS2Point(bPolygon, aPoint);
        }
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
    case ShapeType::LineString: {
      S2Polyline* aLine = static_cast<S2Polyline*>(aRegion);
      switch (b.shape()) {
        case ShapeType::Point: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion)->point();
          return distanceOfS2PolylineWithS2Point(aLine, bPoint);
        }
        case ShapeType::LineString: {
          const S2Polyline* bLine = static_cast<S2Polyline*>(bRegion);
          MutableS2ShapeIndex aIndex, bIndex;
          aIndex.Add(std::make_unique<S2Polyline::Shape>(aLine));
          bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
          S2ClosestEdgeQuery query(&aIndex);
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bIndex);
          return S2Earth::ToMeters(query.GetDistance(&target));
        }
        case ShapeType::Polygon: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion);
          return distanceOfS2PolygonWithS2Polyline(bPolygon, aLine);
        }
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
    }
    case ShapeType::Polygon: {
      S2Polygon* aPolygon = static_cast<S2Polygon*>(aRegion);
      switch (b.shape()) {
        case ShapeType::Point: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion)->point();
          return distanceOfS2PolygonWithS2Point(aPolygon, bPoint);
        }
        case ShapeType::LineString: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion);
          return distanceOfS2PolygonWithS2Polyline(aPolygon, bLine);
        }
        case ShapeType::Polygon: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion);
          S2ClosestEdgeQuery query(&aPolygon->index());
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bPolygon->index());
          return S2Earth::ToMeters(query.GetDistance(&target));
        }
        default:
          LOG(FATAL)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return -1.0;
      }
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
