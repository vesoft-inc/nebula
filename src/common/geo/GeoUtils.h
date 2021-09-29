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
#include "common/geo/GeoShape.h"
#include "common/geo/io/Geometry.h"

namespace nebula {

class GeoUtils final {
 public:
  // We don't check the validity of s2 when converting a Geometry to an S2Region.
  // Therefore, the s2Region we got maybe invalid, and any computation based on it may have
  // undefined behavour. Of course it doesn't cause the program to crash. If the user does not want
  // to tolerate the UB, they could call `ST_IsValid` on the geography data to filter out the
  // illegal ones.
  static std::unique_ptr<S2Region> s2RegionFromGeomtry(const Geometry& geom) {
    switch (geom.shape()) {
      case GeoShape::POINT: {
        const auto& point = geom.point();
        auto s2Point = s2PointFromCoordinate(point.coord);
        return std::make_unique<S2PointRegion>(s2Point);
      }
      case GeoShape::LINESTRING: {
        const auto& lineString = geom.lineString();
        auto coordList = lineString.coordList;
        auto s2Points = s2PointsFromCoordinateList(coordList);
        auto s2Polyline = std::make_unique<S2Polyline>(s2Points, S2Debug::DISABLE);
        return s2Polyline;
      }
      case GeoShape::POLYGON: {
        const auto& polygon = geom.polygon();
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

}  // namespace nebula
