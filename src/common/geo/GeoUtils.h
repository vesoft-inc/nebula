/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

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
        auto coordList = lineString.coordList;
        auto s2Points = s2PointsFromCoordinateList(coordList);
        auto s2Polyline = std::make_unique<S2Polyline>(s2Points, S2Debug::DISABLE);
        return s2Polyline;
      }
      case GeoShape::POLYGON: {
        const auto& polygon = geog.polygon();
        uint32_t numCoordList = polygon.numCoordList();
        std::vector<std::unique_ptr<S2Loop>> s2Loops;
        s2Loops.reserve(numCoordList);
        for (size_t i = 0; i < numCoordList; ++i) {
          auto coordList = polygon.coordListList[i];
          if (!coordList.empty()) {
            coordList.pop_back();  // Remove redundant last coordinate
          }
          auto s2Points = s2PointsFromCoordinateList(coordList);
          auto s2Loop = std::make_unique<S2Loop>(std::move(s2Points), S2Debug::DISABLE);
          s2Loop->Normalize();  // All loops must be oriented CCW(counterclockwise) for S2
          s2Loops.emplace_back(std::move(s2Loop));
        }
        auto s2Polygon = std::make_unique<S2Polygon>(std::move(s2Loops), S2Debug::DISABLE);
        return s2Polygon;
      }
      default:
        LOG(FATAL)
            << "Geography shapes other than Point/LineString/Polygon are not currently supported";
        return nullptr;
    }
  }

  static S2Point s2PointFromCoordinate(const Coordinate& coord) {
    auto latlng = S2LatLng::FromDegrees(
        coord.y, coord.x);  // Note: S2Point requires latitude to be first, and longitude to be last
    return latlng.ToPoint();
  }

  static std::vector<S2Point> s2PointsFromCoordinateList(const std::vector<Coordinate>& coordList) {
    std::vector<S2Point> s2Points;
    uint32_t numCoords = coordList.size();
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
