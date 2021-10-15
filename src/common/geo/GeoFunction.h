/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <s2/s2region_coverer.h>

#include "common/datatypes/Geography.h"

namespace nebula {
namespace geo {

class GeoFunction {
 public:
  // Returns true if any point in the set that comprises A is also a member of the set of points
  // that
  // make up B.
  static bool intersects(const Geography& a, const Geography& b);

  // Returns true if no point in b lies exterior of b.
  // The difference between ST_Covers, ST_Contains and ST_ContainsProperly, see
  // http://lin-ear-th-inking.blogspot.com/2007/06/subtleties-of-ogc-covers-spatial.html
  static bool covers(const Geography& a, const Geography& b);
  static bool coveredBy(const Geography& a, const Geography& b);

  // Returns true if any of a is within distance meters of b.
  // We don't need to find the closest points. We just need to find the first point pair whose
  // distance is less than or less equal than the given distance. (Early quit)
  static bool dWithin(const Geography& a, const Geography& b, double distance, bool inclusive);

  // Return the closest distance in meters of a and b.
  static double distance(const Geography& a, const Geography& b);

  static uint64_t s2CellIdFromPoint(const Geography& a, int level = 30);

  static std::vector<uint64_t> s2CoveringCellIds(const Geography& a,
                                                 int minLevel = 0,
                                                 int maxLevel = 30,
                                                 int maxCells = 8,
                                                 double bufferInMeters = 0.0);

 private:
  static std::vector<uint64_t> coveringCellIds(const S2Region& r,
                                               const S2RegionCoverer::Options& opts);

  static double distanceOfS2PolylineWithS2Point(const S2Polyline* aLine, const S2Point& bPoint);

  static double distanceOfS2PolygonWithS2Polyline(const S2Polygon* aPolygon,
                                                  const S2Polyline* bLine);

  static double distanceOfS2PolygonWithS2Point(const S2Polygon* aPolygon, const S2Point& bPoint);

  static bool s2PointAndS2PolylineAreWithinDistance(const S2Point& aPoint,
                                                    const S2Polyline* bLine,
                                                    double distance,
                                                    bool inclusive);

  static bool s2PointAndS2PolygonAreWithinDistance(const S2Point& aPoint,
                                                   const S2Polygon* bPolygon,
                                                   double distance,
                                                   bool inclusive);

  static bool s2PolylineAndS2PolygonAreWithinDistance(const S2Polyline* aLine,
                                                      const S2Polygon* bPolygon,
                                                      double distance,
                                                      bool inclusive);
};

}  // namespace geo
}  // namespace nebula
