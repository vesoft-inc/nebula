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
#include "common/geo/io/Geometry.h"

namespace nebula {

class GeoUtils final {
 public:
  static std::unique_ptr<S2Region> s2RegionFromGeomtry(const Geometry& geom) {
    switch (geom.shape()) {
      case GeoShape::POINT: {
        const auto& point = geom.point();
        auto s2Point = s2PointFromCoordinate(point.coord);
        if (!S2::IsUnitLength(s2Point)) {  // S2LatLng::IsValid()
          return nullptr;
        }
        return std::make_unique<S2PointRegion>(s2Point);
      }
      case GeoShape::LINESTRING: {
        const auto& lineString = geom.lineString();
        auto coordList = lineString.coordList;
        if (UNLIKELY(coordList.size() < 2)) {
          LOG(ERROR) << "LineString must have at least 2 coordinates";
          return nullptr;
        }
        removeAdjacentDuplicateCoordinates(coordList);
        if (coordList.size() < 2) {
          LOG(ERROR)
              << "Invalid LineString, adjacent coordinates must not be identical or antipodal.";
          return nullptr;
        }

        auto s2Points = s2PointsFromCoordinateList(coordList);
        auto s2Polyline = std::make_unique<S2Polyline>(s2Points, S2Debug::DISABLE);
        if (!s2Polyline->IsValid()) {
          LOG(ERROR) << "Invalid S2Polyline";
          return nullptr;
        }
        return s2Polyline;
      }
      case GeoShape::POLYGON: {
        const auto& polygon = geom.polygon();
        uint32_t numRings = polygon.numRings();
        std::vector<std::unique_ptr<S2Loop>> s2Loops;
        s2Loops.reserve(numRings);
        for (size_t i = 0; i < numRings; ++i) {
          auto coordList = polygon.coordListList[i];
          if (UNLIKELY(coordList.size() < 4)) {
            LOG(ERROR) << "Polygon's LinearRing must have at least 4 coordinates";
            return nullptr;
          }
          if (UNLIKELY(isLoopClosed(coordList))) {
            return nullptr;
          }
          removeAdjacentDuplicateCoordinates(coordList);
          if (coordList.size() < 4) {
            LOG(ERROR)
                << "Invalid linearRing in polygon, must have at least 4 distinct coordinates.";
            return nullptr;
          }
          coordList.pop_back();  // Remove redundant last coordinate
          auto s2Points = s2PointsFromCoordinateList(coordList);
          auto s2Loop = std::make_unique<S2Loop>(std::move(s2Points), S2Debug::DISABLE);
          if (!s2Loop->IsValid()) {  // TODO(jie) May not need to check S2loop here, S2Polygon's
                                     // IsValid() will check its loops
            LOG(ERROR) << "Invalid S2Loop";
            return nullptr;
          }
          s2Loop->Normalize();  // All loops must be oriented CCW(counterclockwise) for S2
          s2Loops.emplace_back(std::move(s2Loop));
        }
        auto s2Polygon = std::make_unique<S2Polygon>(std::move(s2Loops), S2Debug::DISABLE);
        if (!s2Polygon->IsValid()) {  // Exterior loop must contain other interior loops
          LOG(ERROR) << "Invalid S2Polygon";
          return nullptr;
        }
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

  static bool isLoopClosed(const std::vector<Coordinate>& coordList) {
    if (coordList.size() < 2) {
      return false;
    }
    return coordList.front() == coordList.back();
  }

  static void removeAdjacentDuplicateCoordinates(std::vector<Coordinate>& coordList) {
    if (coordList.size() < 2) {
      return;
    }

    auto last = std::unique(coordList.begin(), coordList.end());
    coordList.erase(last, coordList.end());
  }
};

}  // namespace nebula
