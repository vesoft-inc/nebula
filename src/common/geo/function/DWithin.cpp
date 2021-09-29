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
  if (!aRegion || !bRegion) {
    LOG(INFO) << "dWithin(), asS2() failed.";
    return false;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      const S2Point& aPoint = static_cast<S2PointRegion*>(aRegion.get())->point();
      switch (b.shape()) {
        case GeoShape::POINT: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion.get())->point();
          double closestDistance = S2Earth::GetDistanceMeters(aPoint, bPoint);
          return inclusive ? closestDistance <= distance : closestDistance < distance;
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          return s2PointAndS2PolylineAreWithinDistance(aPoint, bLine, distance, inclusive);
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          return s2PointAndS2PolygonAreWithinDistance(aPoint, bPolygon, distance, inclusive);
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
          return s2PointAndS2PolylineAreWithinDistance(bPoint, aLine, distance, inclusive);
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
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
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          return s2PolylineAndS2PolygonAreWithinDistance(aLine, bPolygon, distance, inclusive);
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
          return s2PointAndS2PolygonAreWithinDistance(bPoint, aPolygon, distance, inclusive);
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          return s2PolylineAndS2PolygonAreWithinDistance(bLine, aPolygon, distance, inclusive);
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          S2ClosestEdgeQuery query(&aPolygon->index());
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bPolygon->index());
          if (inclusive) {
            return query.IsDistanceLessOrEqual(
                &target, S2Earth::ToChordAngle(util::units::Meters(distance)));
          }
          return query.IsDistanceLess(&target,
                                      S2Earth::ToChordAngle(util::units::Meters(distance)));
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
