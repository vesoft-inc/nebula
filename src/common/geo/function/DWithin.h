/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/datatypes/Geography.h"

namespace nebula {

// We don't need to find the closest points. We just need to find the first point pair whose
// distance is less than or less equal than the given distance. (Early quit)
bool dWithin(const Geography& a, const Geography& b, double distance, bool inclusive);

bool s2PointAndS2PolylineAreWithinDistance(const S2Point& aPoint,
                                           const S2Polyline* bLine,
                                           double distance,
                                           bool inclusive);

bool s2PointAndS2PolygonAreWithinDistance(const S2Point& aPoint,
                                          const S2Polygon* bPolygon,
                                          double distance,
                                          bool inclusive);

bool s2PolylineAndS2PolygonAreWithinDistance(const S2Polyline* aLine,
                                             const S2Polygon* bPolygon,
                                             double distance,
                                             bool inclusive);

}  // namespace nebula
