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
  static std::unique_ptr<S2Region> s2RegionFromGeomtry(const Geometry* geom) {
    switch (geom->shape()) {
      case GeoShape::POINT: {
        const auto* point = static_cast<const Point*>(geom);
        auto s2Point = s2PointFromCoordinate(point->coord);
        return std::make_unique<S2PointRegion>(s2Point);
      }
      case GeoShape::LINESTRING: {
        const auto* lineString = static_cast<const LineString*>(geom);
        auto coordList = lineString->coordList;
        DCHECK_GE(coordList.size(), 2);
        removeAdjacentDuplicateCoordinates(coordList);
        if (coordList.size() < 2) {
          LOG(INFO)
              << "Invalid LineString, adjacent coordinates must not be identical or antipodal.";
          return nullptr;
        }

        auto s2Points = s2PointsFromCoordinateList(coordList);
        auto s2Polyline = std::make_unique<S2Polyline>(s2Points, S2Debug::DISABLE);
        if (!s2Polyline->IsValid()) {
          return nullptr;
        }
        return s2Polyline;
      }
      case GeoShape::POLYGON: {
        const auto* polygon = static_cast<const Polygon*>(geom);
        uint32_t numRings = polygon->numRings();
        std::vector<std::unique_ptr<S2Loop>> s2Loops;
        s2Loops.reserve(numRings);
        for (size_t i = 0; i < numRings; ++i) {
          auto coordList = polygon->coordListList[i];
          DCHECK_GE(coordList.size(), 4);
          DCHECK(isLoopClosed(coordList));
          removeAdjacentDuplicateCoordinates(coordList);
          if (coordList.size() < 4) {
            LOG(INFO)
                << "Invalid linearRing in polygon, must have at least 4 distinct coordinates.";
            return nullptr;
          }
          coordList.pop_back();  // Remove redundant last coordinate
          auto s2Points = s2PointsFromCoordinateList(coordList);
          auto* s2Loop = new S2Loop(std::move(s2Points), S2Debug::DISABLE);
          if (!s2Loop->IsValid()) {
            return nullptr;
          }
          s2Loop->Normalize();  // All loops must be oriented CCW(counterclockwise) for S2
          s2Loops.emplace_back(s2Loop);
        }
        auto s2Polygon = std::make_unique<S2Polygon>(std::move(s2Loops), S2Debug::DISABLE);
        if (!s2Polygon->IsValid()) {  // Exterior loop must contain other interior loops
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
    DCHECK(latlng.is_valid());
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
    DCHECK_GE(coordList.size(), 2);
    return coordList.front() == coordList.back();
  }

  static void removeAdjacentDuplicateCoordinates(std::vector<Coordinate>& coordList) {
    if (coordList.size() < 2) {
      return;
    }

    size_t i = 0, j = 1;
    for (; j < coordList.size(); ++j) {
      if (coordList[i] != coordList[j]) {
        ++i;
        if (i != j) {  // i is always <= j
          coordList[i] = coordList[j];
        }
      }
    }
  }
};

}  // namespace nebula
