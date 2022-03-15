/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/geo/GeoFunction.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2closest_edge_query.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2shape_index_buffered_region.h>

namespace nebula {
namespace geo {

bool GeoFunction::intersects(const Geography& a, const Geography& b) {
  auto aRegion = a.asS2();
  auto bRegion = b.asS2();
  if (UNLIKELY(!aRegion || !bRegion)) {
    return false;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      switch (b.shape()) {
        case GeoShape::POINT:
          return static_cast<S2PointRegion*>(aRegion.get())
              ->MayIntersect(S2Cell(static_cast<S2PointRegion*>(bRegion.get())->point()));
        case GeoShape::LINESTRING:
          return static_cast<S2Polyline*>(bRegion.get())
              ->MayIntersect(S2Cell(static_cast<S2PointRegion*>(aRegion.get())->point()));
        case GeoShape::POLYGON:
          return static_cast<S2Polygon*>(bRegion.get())
              ->MayIntersect(S2Cell(static_cast<S2PointRegion*>(aRegion.get())->point()));
        case GeoShape::UNKNOWN:
        default: {
          LOG(ERROR)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return false;
        }
      }
    }
    case GeoShape::LINESTRING: {
      switch (b.shape()) {
        case GeoShape::POINT:
          return static_cast<S2Polyline*>(aRegion.get())
              ->MayIntersect(S2Cell(static_cast<S2PointRegion*>(bRegion.get())->point()));
        case GeoShape::LINESTRING:
          return static_cast<S2Polyline*>(aRegion.get())
              ->Intersects(static_cast<S2Polyline*>(bRegion.get()));
        case GeoShape::POLYGON:
          return static_cast<S2Polygon*>(bRegion.get())
              ->Intersects(*static_cast<S2Polyline*>(aRegion.get()));
        case GeoShape::UNKNOWN:
        default: {
          LOG(ERROR)
              << "Geography shapes other than Point/LineString/Polygon are not currently supported";
          return false;
        }
      }
    }
    case GeoShape::POLYGON: {
      switch (b.shape()) {
        case GeoShape::POINT:
          return static_cast<S2Polygon*>(aRegion.get())
              ->MayIntersect(S2Cell(static_cast<S2PointRegion*>(bRegion.get())->point()));
        case GeoShape::LINESTRING:
          return static_cast<S2Polygon*>(aRegion.get())
              ->Intersects(*static_cast<S2Polyline*>(bRegion.get()));
        case GeoShape::POLYGON:
          return static_cast<S2Polygon*>(aRegion.get())
              ->Intersects(static_cast<S2Polygon*>(bRegion.get()));
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

bool GeoFunction::covers(const Geography& a, const Geography& b) {
  auto aRegion = a.asS2();
  auto bRegion = b.asS2();
  if (UNLIKELY(!aRegion || !bRegion)) {
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

bool GeoFunction::coveredBy(const Geography& a, const Geography& b) {
  return covers(b, a);
}

bool GeoFunction::dWithin(const Geography& a, const Geography& b, double distance, bool exclusive) {
  auto aRegion = a.asS2();
  auto bRegion = b.asS2();
  if (UNLIKELY(!aRegion || !bRegion)) {
    return false;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      const S2Point& aPoint = static_cast<S2PointRegion*>(aRegion.get())->point();
      switch (b.shape()) {
        case GeoShape::POINT: {
          const S2Point& bPoint = static_cast<S2PointRegion*>(bRegion.get())->point();
          double closestDistance = S2Earth::GetDistanceMeters(aPoint, bPoint);
          return exclusive ? closestDistance < distance : closestDistance <= distance;
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          return s2PointAndS2PolylineAreWithinDistance(aPoint, bLine, distance, exclusive);
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          return s2PointAndS2PolygonAreWithinDistance(aPoint, bPolygon, distance, exclusive);
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
          return s2PointAndS2PolylineAreWithinDistance(bPoint, aLine, distance, exclusive);
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          MutableS2ShapeIndex aIndex, bIndex;
          aIndex.Add(std::make_unique<S2Polyline::Shape>(aLine));
          bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
          S2ClosestEdgeQuery query(&aIndex);
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bIndex);
          if (exclusive) {
            return query.IsDistanceLess(&target,
                                        S2Earth::ToChordAngle(util::units::Meters(distance)));
          }
          return query.IsDistanceLessOrEqual(&target,
                                             S2Earth::ToChordAngle(util::units::Meters(distance)));
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          return s2PolylineAndS2PolygonAreWithinDistance(aLine, bPolygon, distance, exclusive);
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
          return s2PointAndS2PolygonAreWithinDistance(bPoint, aPolygon, distance, exclusive);
        }
        case GeoShape::LINESTRING: {
          S2Polyline* bLine = static_cast<S2Polyline*>(bRegion.get());
          return s2PolylineAndS2PolygonAreWithinDistance(bLine, aPolygon, distance, exclusive);
        }
        case GeoShape::POLYGON: {
          S2Polygon* bPolygon = static_cast<S2Polygon*>(bRegion.get());
          S2ClosestEdgeQuery query(&aPolygon->index());
          S2ClosestEdgeQuery::ShapeIndexTarget target(&bPolygon->index());
          if (exclusive) {
            return query.IsDistanceLess(&target,
                                        S2Earth::ToChordAngle(util::units::Meters(distance)));
          }
          return query.IsDistanceLessOrEqual(&target,
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

double GeoFunction::distance(const Geography& a, const Geography& b) {
  auto aRegion = a.asS2();
  auto bRegion = b.asS2();
  if (UNLIKELY(!aRegion || !bRegion)) {
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
          return -1.0;
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
          return -1.0;
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
          return -1.0;
        }
      }
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return -1.0;
    }
  }

  return -1.0;
}

uint64_t GeoFunction::s2CellIdFromPoint(const Geography& a, int level) {
  auto aRegion = a.asS2();
  if (UNLIKELY(!aRegion)) {
    return -1;
  }
  if (level < 0 || level > 30) {
    LOG(ERROR) << "s2 level argument must be in range [0,30], got " << level;
    return -1;
  }

  switch (a.shape()) {
    case GeoShape::POINT: {
      const auto& s2Point = static_cast<S2PointRegion*>(aRegion.get())->point();
      S2CellId cellId(s2Point);
      return level == 30 ? cellId.id() : cellId.parent(level).id();
    }
    case GeoShape::LINESTRING:
    case GeoShape::POLYGON:
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR) << "S2_CellIdFromPoint only accept Point";
      return -1;
    }
  }

  return false;
}

std::vector<uint64_t> GeoFunction::s2CoveringCellIds(
    const Geography& a, int minLevel, int maxLevel, int maxCells, double bufferInMeters) {
  auto aRegion = a.asS2();
  if (UNLIKELY(!aRegion)) {
    return {};
  }
  if (minLevel < 0 || minLevel > 30) {
    LOG(ERROR) << "s2 min_level argument must be in range [0,30], got " << minLevel;
    return {};
  }
  if (maxLevel < 0 || maxLevel > 30) {
    LOG(ERROR) << "s2 max_level argument must be in range [0,30], got " << maxLevel;
    return {};
  }
  if (maxCells <= 0) {
    LOG(ERROR) << "s2 max_cells argument must be greater than or equal to 0, got " << maxCells;
    return {};
  }
  if (bufferInMeters < 0.0) {
    LOG(ERROR) << "s2 buffer_meters argument must be nonnegative, got " << bufferInMeters;
    return {};
  }

  S2RegionCoverer::Options opts;
  opts.set_min_level(minLevel);
  opts.set_max_level(maxLevel);
  opts.set_max_cells(maxCells);

  if (bufferInMeters == 0.0) {
    if (a.shape() == GeoShape::POINT) {
      const S2Point& gPoint = static_cast<const S2PointRegion*>(aRegion.get())->point();
      return {S2CellId(gPoint).id()};
    }
    return coveringCellIds(*aRegion, opts);
  }

  S1Angle radius = S2Earth::ToAngle(util::units::Meters(bufferInMeters));

  switch (a.shape()) {
    case GeoShape::POINT: {
      const S2Point& gPoint = static_cast<S2PointRegion*>(aRegion.get())->point();
      S2Cap gCap(gPoint, radius);
      return coveringCellIds(gCap, opts);
    }
    case GeoShape::LINESTRING: {
      S2Polyline* gLine = static_cast<S2Polyline*>(aRegion.get());
      MutableS2ShapeIndex index;
      index.Add(std::make_unique<S2Polyline::Shape>(gLine));
      S2ShapeIndexBufferedRegion gBuffer(&index, radius);
      return coveringCellIds(gBuffer, opts);
    }
    case GeoShape::POLYGON: {
      S2Polygon* gPolygon = static_cast<S2Polygon*>(aRegion.get());
      S2ShapeIndexBufferedRegion gBuffer(&gPolygon->index(), radius);
      return coveringCellIds(gBuffer, opts);
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return {};
    }
  }

  return {};
}

std::vector<uint64_t> GeoFunction::coveringCellIds(const S2Region& r,
                                                   const S2RegionCoverer::Options& opts) {
  S2RegionCoverer rc(opts);
  std::vector<S2CellId> covering;
  rc.GetCovering(r, &covering);
  std::vector<uint64_t> cellIds;
  cellIds.reserve(covering.size());
  for (auto& cellId : covering) {
    cellIds.push_back(cellId.id());
  }
  return cellIds;
}

double GeoFunction::distanceOfS2PolylineWithS2Point(const S2Polyline* aLine,
                                                    const S2Point& bPoint) {
  int tmp;
  S2Point closestPointOnLine = aLine->Project(bPoint, &tmp);
  return S2Earth::GetDistanceMeters(closestPointOnLine, bPoint);
}

double GeoFunction::distanceOfS2PolygonWithS2Polyline(const S2Polygon* aPolygon,
                                                      const S2Polyline* bLine) {
  S2ClosestEdgeQuery query(&aPolygon->index());
  MutableS2ShapeIndex bIndex;
  bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
  S2ClosestEdgeQuery::ShapeIndexTarget target(&bIndex);
  return S2Earth::ToMeters(query.GetDistance(&target));
}

double GeoFunction::distanceOfS2PolygonWithS2Point(const S2Polygon* aPolygon,
                                                   const S2Point& bPoint) {
  return S2Earth::ToMeters(aPolygon->GetDistance(bPoint));
}

bool GeoFunction::s2PointAndS2PolylineAreWithinDistance(const S2Point& aPoint,
                                                        const S2Polyline* bLine,
                                                        double distance,
                                                        bool exclusive) {
  MutableS2ShapeIndex bIndex;
  bIndex.Add(std::make_unique<S2Polyline::Shape>(bLine));
  S2ClosestEdgeQuery query(&bIndex);
  S2ClosestEdgeQuery::PointTarget target(aPoint);
  if (exclusive) {
    return query.IsDistanceLess(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
  }
  return query.IsDistanceLessOrEqual(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
}

bool GeoFunction::s2PointAndS2PolygonAreWithinDistance(const S2Point& aPoint,
                                                       const S2Polygon* bPolygon,
                                                       double distance,
                                                       bool exclusive) {
  S2ClosestEdgeQuery query(&bPolygon->index());
  S2ClosestEdgeQuery::PointTarget target(aPoint);
  if (exclusive) {
    return query.IsDistanceLess(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
  }
  return query.IsDistanceLessOrEqual(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
}

bool GeoFunction::s2PolylineAndS2PolygonAreWithinDistance(const S2Polyline* aLine,
                                                          const S2Polygon* bPolygon,
                                                          double distance,
                                                          bool exclusive) {
  MutableS2ShapeIndex aIndex;
  aIndex.Add(std::make_unique<S2Polyline::Shape>(aLine));
  S2ClosestEdgeQuery::ShapeIndexTarget target(&aIndex);
  S2ClosestEdgeQuery query(&bPolygon->index());
  if (exclusive) {
    return query.IsDistanceLess(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
  }
  return query.IsDistanceLessOrEqual(&target, S2Earth::ToChordAngle(util::units::Meters(distance)));
}

}  // namespace geo
}  // namespace nebula
