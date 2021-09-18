/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/datatypes/Geography.h"

namespace nebula {

// Find the closest distance of a and b
double distance(const Geography& a, const Geography& b);

double distanceOfS2PolylineWithS2Point(const S2Polyline* aLine, const S2Point& bPoint);

double distanceOfS2PolygonWithS2Polyline(const S2Polygon* aPolygon, const S2Polyline* bLine);

double distanceOfS2PolygonWithS2Point(const S2Polygon* aPolygon, const S2Point& bPoint);

}  // namespace nebula
