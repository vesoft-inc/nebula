/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_GEO_GEOUTILS_H
#define COMMON_GEO_GEOUTILS_H

#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>

#include "common/base/StatusOr.h"
#include "common/datatypes/Geography.h"

namespace nebula {
namespace geo {

class GeoUtils final {
 public:
  static std::unique_ptr<S2Region> s2RegionFromGeography(Geography geog) {
    switch (geog.shape()) {
      case GeoShape::POINT: {
        const auto& point = geog.point();
        auto s2Point = s2PointFromCoordinate(point.coord);
        return std::make_unique<S2PointRegion>(s2Point);
      }
      case GeoShape::LINESTRING: {
        const auto& lineString = geog.lineString();
        const auto& coordList = lineString.coordList;
        auto s2Points = s2PointsFromCoordinateList(coordList);
        return std::make_unique<S2Polyline>(s2Points, S2Debug::DISABLE);
      }
      case GeoShape::POLYGON: {
        const auto& polygon = geog.polygon();
        uint32_t numCoordList = polygon.numCoordList();
        std::vector<std::unique_ptr<S2Loop>> s2Loops;
        s2Loops.reserve(numCoordList);
        for (const auto& coordList : polygon.coordListList) {
          // S2 doesn't need the redundant last point
          auto s2Points = s2PointsFromCoordinateList(coordList, true);
          auto s2Loop = std::make_unique<S2Loop>(std::move(s2Points), S2Debug::DISABLE);
          // All loops must be oriented CCW(counterclockwise) for S2
          s2Loop->Normalize();
          s2Loops.emplace_back(std::move(s2Loop));
        }
        return std::make_unique<S2Polygon>(std::move(s2Loops), S2Debug::DISABLE);
      }
      default:
        LOG(FATAL)
            << "Geography shapes other than Point/LineString/Polygon are not currently supported";
        return nullptr;
    }
  }

  static S2Point s2PointFromCoordinate(const Coordinate& coord) {
    // Note: S2Point requires latitude to be first, and longitude to be last
    auto latlng = S2LatLng::FromDegrees(coord.y, coord.x);
    return latlng.ToPoint();
  }

  static Coordinate coordinateFromS2Point(const S2Point& s2Point) {
    S2LatLng s2Latlng(s2Point);
    return Coordinate(s2Latlng.lng().degrees(), s2Latlng.lat().degrees());
  }

  static std::vector<S2Point> s2PointsFromCoordinateList(const std::vector<Coordinate>& coordList,
                                                         bool excludeTheLast = false) {
    std::vector<S2Point> s2Points;
    uint32_t numCoords = coordList.size();
    if (excludeTheLast) {
      numCoords -= 1;
    }
    if (numCoords == 0) {
      return {};
    }

    s2Points.reserve(numCoords);
    for (size_t i = 0; i < numCoords; ++i) {
      auto coord = coordList[i];
      auto s2Point = s2PointFromCoordinate(coord);
      s2Points.emplace_back(s2Point);
    }

    return s2Points;
  }

  static void removeAdjacentDuplicateCoordinates(std::vector<Coordinate>& coordList) {
    if (coordList.size() < 2) {
      return;
    }

    auto last = std::unique(coordList.begin(), coordList.end());
    coordList.erase(last, coordList.end());
  }
};

}  // namespace geo
}  // namespace nebula
#endif
