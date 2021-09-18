/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/geo/function/DWithin.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2closest_edge_query.h>
#include <s2/s2earth.h>
#include <s2/s2polygon.h>

namespace nebula {

// We don't need to find the closest points. We just need to find the first point pair whose
// distance is less than or less equal than the given distance. (Early quit)
bool dWithin(const Geography& a, const Geography& b, double distance, bool inclusive) {
  auto aRegion = a.asS2();
  auto bRegion = b.asS2();

  switch (a.shape()) {
    case ShapeType::Point: {
      const S2Point& aPoint = static_cast<S2PointRegion*>(aRegion)->point();
      switch (b.shape()) {
        case ShapeType::Point: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion)->point();
          double closestDistance = S2Earth::GetDistanceMeters(aPoint, bPoint);
          return inclusive ? closestDistance <= distance : closestDistance < distance;
        }
        case ShapeType::LineString: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion);
          return s2PointAndS2PolylineAreWithinDistance(aPoint, bLine, distance, inclusive);
        }
        case ShapeType::Polygon: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion);
          return s2PointAndS2PolygonAreWithinDistance(aPoint, bPolygon, distance, inclusive);
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
          return s2PointAndS2PolylineAreWithinDistance(bPoint, aLine, distance, inclusive);
        }
        case ShapeType::LineString: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion);
          MutableS2ShapeIndex aIndex, bIndex;
          aIndex.Add(std::make_unique<S2Polyline::Shape>(aLine));
          bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
          S2ClosestEdgeQuery query(&aIndex);
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bIndex);
          if (inclusive) {
            return query.IsDistanceLessOrEqual(
                &target, S2Earth::ToChordAngle(util::units::Meters(distance)));
          }
          return query.IsDistanceLess(&target,
                                      S2Earth::ToChordAngle(util::units::Meters(distance)));
        }
        case ShapeType::Polygon: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion);
          return s2PolylineAndS2PolygonAreWithinDistance(aLine, bPolygon, distance, inclusive);
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
          return s2PointAndS2PolygonAreWithinDistance(bPoint, aPolygon, distance, inclusive);
        }
        case ShapeType::LineString: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion);
          return s2PolylineAndS2PolygonAreWithinDistance(bLine, aPolygon, distance, inclusive);
        }
        case ShapeType::Polygon: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion);
          S2ClosestEdgeQuery query(&aPolygon->index());
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bPolygon->index());
          if (inclusive) {
            return query.IsDistanceLessOrEqual(
                &target, S2Earth::ToChordAngle(util::units::Meters(distance)));
          }
          return query.IsDistanceLess(&target,
                                      S2Earth::ToChordAngle(util::units::Meters(distance)));
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

bool s2PointAndS2PolylineAreWithinDistance(const S2Point& aPoint,
                                           const S2Polyline* bLine,
                                           double distance,
                                           bool inclusive) {
  MutableS2ShapeIndex bIndex;
  bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
  S2ClosestEdgeQuery query(&bIndex);
  S2ClosestEdgeQuery::PointTarget target(aPoint);
  if (inclusive) {
    return query.IsDistanceLessOrEqual(&target,
                                       S2Earth::ToChordAngle(util::units::Meters(distance)));
  } else {
    return query.IsDistanceLess(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
  }
}

bool s2PointAndS2PolygonAreWithinDistance(const S2Point& aPoint,
                                          const S2Polygon* bPolygon,
                                          double distance,
                                          bool inclusive) {
  S2ClosestEdgeQuery query(&bPolygon->index());
  S2ClosestEdgeQuery::PointTarget target(aPoint);
  if (inclusive) {
    return query.IsDistanceLessOrEqual(&target,
                                       S2Earth::ToChordAngle(util::units::Meters(distance)));
  } else {
    return query.IsDistanceLess(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
  }
}

bool s2PolylineAndS2PolygonAreWithinDistance(const S2Polyline* aLine,
                                             const S2Polygon* bPolygon,
                                             double distance,
                                             bool inclusive) {
  MutableS2ShapeIndex aIndex;
  aIndex.Add(std::make_unique<S2Polyline::Shape>(aLine));
  S2ClosestEdgeQuery::ShapeIndexTarget target(&aIndex);
  S2ClosestEdgeQuery query(&bPolygon->index());
  if (inclusive) {
    return query.IsDistanceLessOrEqual(&target,
                                       S2Earth::ToChordAngle(util::units::Meters(distance)));
  } else {
    return query.IsDistanceLess(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
  }
}

}  // namespace nebula
