/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/geo/GeoFunction.h"

namespace nebula {
namespace geo {

TEST(Intersects, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = GeoFunction::intersects(point1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = GeoFunction::intersects(point1, point2);
    EXPECT_EQ(false, b);
  }
  // {
  //   auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
  //   auto point2 = Geography::fromWKT("POINT(1.0 1.00000000001)").value();
  //   bool b = GeoFunction::intersects(point1, point2);
  //   EXPECT_EQ(false, b); // The error of GeoFunction::intersects should be 1e-11
  // }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.000000000001)").value();
    bool b = GeoFunction::intersects(point1, point2);
    EXPECT_EQ(true, b);
  }
}

TEST(Intersects, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = GeoFunction::intersects(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(3.0 3.000000000001, 2.0 2.0)").value();
    bool b = GeoFunction::intersects(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = GeoFunction::intersects(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.2 3.8)").value();
    auto point2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = GeoFunction::intersects(point1, point2);
    EXPECT_EQ(false, b);
  }
}

TEST(Intersects, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = GeoFunction::intersects(point1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.000000000001)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = GeoFunction::intersects(point1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.9)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = GeoFunction::intersects(point1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.3 0.3)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::intersects(point1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(Intersects, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = GeoFunction::intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = GeoFunction::intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.5499860 1.5501575, 1.5 1.5001714)").value();
    bool b = GeoFunction::intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 2.0 1.0)").value();
    bool b = GeoFunction::intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 1.0 4.0)").value();
    bool b = GeoFunction::intersects(line1, line2);
    EXPECT_EQ(false, b);
  }
}

TEST(Intersects, lineString2Polygon) {
  // {
  //   auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
  //   auto polygon2 =
  //       Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
  //   bool b = GeoFunction::intersects(line1, polygon2);
  //   EXPECT_EQ(true, b); // Expect true, got false
  // }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.2 0.2, 0.4 0.4)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 1.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, -1.5 -1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::intersects(line1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.35 0.35)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::intersects(line1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.5 0.5)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.4 0.3, 0.6 0.3)").value();
    auto polygon2 =
        Geography::fromWKT(
            "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.3 0.3, 0.4 0.2, 0.5 0.3, "
            "0.4 0.4, 0.3 0.3), (0.5 0.3, 0.6 0.2, 0.7 0.3, 0.6 0.4, 0.5 0.3))")
            .value();
    bool b = GeoFunction::intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
}

TEST(Intersects, polygon2Polygon) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    bool b = GeoFunction::intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))").value();
    bool b = GeoFunction::intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }

  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))").value();
    bool b = GeoFunction::intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }

  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    bool b = GeoFunction::intersects(polygon1, polygon2);
    EXPECT_EQ(false, b);
  }

  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))").value();
    bool b = GeoFunction::intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
}

TEST(Covers, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = GeoFunction::covers(point1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = GeoFunction::covers(point1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.00000000001)").value();
    bool b = GeoFunction::covers(point1, point2);
    EXPECT_EQ(false, b);
  }
  // {
  //   auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
  //   auto point2 = Geography::fromWKT("POINT(1.0 1.000000000001)").value();
  //   bool b = GeoFunction::covers(point1, point2);
  //   EXPECT_EQ(true, b); // The error should be 1e-11?
  // }
}

TEST(Covers, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = GeoFunction::covers(point1, line2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = GeoFunction::covers(point1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, lineString2Point) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = GeoFunction::covers(line1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(2.0 2.0)").value();
    bool b = GeoFunction::covers(line1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(3.0 3.0)").value();
    bool b = GeoFunction::covers(line1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(2.0 4.0)").value();
    bool b = GeoFunction::covers(line1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.5 1.5001714)").value();
    bool b = GeoFunction::covers(line1, point2);
    EXPECT_EQ(true, b);  // false
  }
}

TEST(Covers, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    bool b = GeoFunction::covers(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(4.0 4.0, 3.0 3.0, 2.0 2.0)").value();
    bool b = GeoFunction::covers(line1, line2);
    EXPECT_EQ(true, b);  // Line should GeoFunction::covers its reverse
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    bool b = GeoFunction::covers(line1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    bool b = GeoFunction::covers(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(5.0 5.0, 4.0 4.0)").value();
    bool b = GeoFunction::covers(line1, line2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, lineString2Polygon) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::covers(line1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, polygon2Point) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto point2 = Geography::fromWKT("POINT(0.5 0.5)").value();
    bool b = GeoFunction::covers(polygon1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto point2 = Geography::fromWKT("POINT(0.5 7.7)").value();
    bool b = GeoFunction::covers(polygon1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto point2 = Geography::fromWKT("POINT(0.3 0.3)").value();
    bool b = GeoFunction::covers(polygon1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto point2 = Geography::fromWKT("POINT(1.0 0.0)").value();
    bool b = GeoFunction::covers(polygon1, point2);
    EXPECT_EQ(true, b);
  }
}

TEST(Covers, polygon2LineString) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(0.1 0.1, 0.4 0.4)").value();
    bool b = GeoFunction::covers(polygon1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto line2 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.35 0.35)").value();
    bool b = GeoFunction::covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto line2 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.55 0.55)").value();
    bool b = GeoFunction::covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto line2 = Geography::fromWKT("LINESTRING(0.3 0.3, 4.0 4.0)").value();
    bool b = GeoFunction::covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(-1 -1, -2 -2)").value();
    bool b = GeoFunction::covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(-0.5 -0.5, 0.5 0.5)").value();
    bool b = GeoFunction::covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(0.0 0.0, 1.0 0.0)").value();
    bool b = GeoFunction::covers(polygon1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 0.0, 0.0 0.0)").value();
    bool b = GeoFunction::covers(
        polygon1, line2);  // The line is equal to the first edge of the polygon, but their vertices
                           // are in reverse order. (0.0, 0.0, 1.0 0.0), (1.0 0.0, 0.0 0.0)
    EXPECT_EQ(true, b);
  }
}

TEST(Covers, polygon2Polygon) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::covers(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    bool b = GeoFunction::covers(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))").value();
    bool b = GeoFunction::covers(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))").value();
    bool b = GeoFunction::covers(polygon1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    bool b = GeoFunction::covers(polygon1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(CoveredBy, basic) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = GeoFunction::coveredBy(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    bool b = GeoFunction::coveredBy(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::coveredBy(line1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 1.0, 1.5 1.5, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::coveredBy(line1, polygon2);
    EXPECT_EQ(true, b);
  }
}

TEST(DWithin, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = GeoFunction::dWithin(point1, point2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = GeoFunction::dWithin(point1, point2, 0.0, true);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = GeoFunction::dWithin(point1, point2, 0.1, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = GeoFunction::dWithin(point1, point2, 22239.020235496788, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = GeoFunction::dWithin(point1, point2, 22239.020235496788, true);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = GeoFunction::dWithin(point1, point2, 22240, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(-118.4079 33.9434)").value();
    auto point2 = Geography::fromWKT("POINT(2.5559 49.0083)").value();
    bool b = GeoFunction::dWithin(point1, point2, 10000, true);
    EXPECT_EQ(false, b);
  }
}

TEST(DWithin, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = GeoFunction::dWithin(point1, line2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = GeoFunction::dWithin(point1, line2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.2, 1.5 1.6)").value();
    bool b = GeoFunction::dWithin(point1, line2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.2 3.8)").value();
    auto point2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = GeoFunction::dWithin(point1, point2, 1.0, true);
    EXPECT_EQ(false, b);
  }
}

TEST(DWithin, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = GeoFunction::dWithin(point1, polygon2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  // Point lies on the interior of the polygon
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.9)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = GeoFunction::dWithin(point1, polygon2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::dWithin(point1, polygon2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::dWithin(point1, polygon2, 99999, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.3 0.3)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::dWithin(point1, polygon2, 10000, false);
    EXPECT_EQ(false, b);
  }
}

TEST(DWithin, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = GeoFunction::dWithin(line1, line2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = GeoFunction::dWithin(line1, line2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.0 0.0, 1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(5.0 5.0, 6.0 6.0)").value();
    bool b = GeoFunction::dWithin(line1, line2, 628519, false);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 2.0 1.0)").value();
    bool b = GeoFunction::dWithin(line1, line2, 1.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 1.0 4.0)").value();
    bool b = GeoFunction::dWithin(line1, line2, 78609, true);
    EXPECT_EQ(true, b);
  }
}

TEST(DWithin, lineString2Polygon) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::dWithin(line1, polygon2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.2 0.2, 0.4 0.4)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::dWithin(line1, polygon2, 0.0, true);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::dWithin(line1, polygon2, 9999, true);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.35 0.35)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::dWithin(line1, polygon2, 5555, false);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.6 0.6)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::dWithin(line1, polygon2, 0.1, true);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(7.0 7.0, 5.0 5.0)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::dWithin(line1, polygon2, 628520, true);
    EXPECT_EQ(true, b);
  }
}

TEST(DWithin, polygon2Polygon) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = GeoFunction::dWithin(polygon1, polygon2, 0.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    bool b = GeoFunction::dWithin(polygon1, polygon2, 1.1, true);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    bool b = GeoFunction::dWithin(polygon1, polygon2, 314402, false);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.25 0.25, 0.35 0.25, 0.35 0.35, 0.25 0.35, 0.25 0.25))")
            .value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = GeoFunction::dWithin(polygon1, polygon2, 5560, true);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-8.0 -8.0, -4.0 -8.0, -4.0 -4.0, -8.0 -4.0, -8.0 -8.0))")
            .value();
    bool b = GeoFunction::dWithin(polygon1, polygon2, 628750, true);
    EXPECT_EQ(false, b);
  }
}

TEST(Distance, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    double d = GeoFunction::distance(point1, point2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    double d = GeoFunction::distance(point1, point2);
    EXPECT_EQ(22239.020235496788, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.00001)").value();
    double d = GeoFunction::distance(point1, point2);
    EXPECT_EQ(1.1119510117584994, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(-118.4079 33.9434)").value();
    auto point2 = Geography::fromWKT("POINT(2.5559 49.0083)").value();
    double d = GeoFunction::distance(point1, point2);
    EXPECT_EQ(9103089.738448681, d);
  }
}

TEST(Distance, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    double d = GeoFunction::distance(point1, line2);
    EXPECT_EQ(0.0, d);
  }
  // {
  //   auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
  //   auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
  //   double d = GeoFunction::distance(point1, line2);
  //   EXPECT_EQ(0.0, d);  // Expect 0.0, got 0.000000000031290120680325526
  // }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.2, 1.5 1.6)").value();
    double d = GeoFunction::distance(point1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.2 3.8)").value();
    auto point2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    double d = GeoFunction::distance(point1, point2);
    EXPECT_EQ(198856.0525640426, d);
  }
}

TEST(Distance, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    double d = GeoFunction::distance(point1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  // Point lies on the interior of the polygon
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.9)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    double d = GeoFunction::distance(point1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = GeoFunction::distance(point1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = GeoFunction::distance(point1, polygon2);
    EXPECT_EQ(86836.82688844757, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.3 0.3)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = GeoFunction::distance(point1, polygon2);
    EXPECT_EQ(11119.357694100196, d);
  }
}

TEST(Distance, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    double d = GeoFunction::distance(line1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    double d = GeoFunction::distance(line1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.0 0.0, 1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(5.0 5.0, 6.0 6.0)").value();
    double d = GeoFunction::distance(line1, line2);
    EXPECT_EQ(628519.1549911008, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 2.0 1.0)").value();
    double d = GeoFunction::distance(line1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 1.0 4.0)").value();
    double d = GeoFunction::distance(line1, line2);
    EXPECT_EQ(78608.33038471815, d);
  }
}

TEST(Distance, lineString2Polygon) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = GeoFunction::distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.2 0.2, 0.4 0.4)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = GeoFunction::distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = GeoFunction::distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.35 0.35)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = GeoFunction::distance(line1, polygon2);
    EXPECT_EQ(5559.651326278197, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.6 0.6)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = GeoFunction::distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(7.0 7.0, 5.0 5.0)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = GeoFunction::distance(line1, polygon2);
    EXPECT_EQ(628519.1549911008, d);
  }
}

TEST(Distance, polygon2Polygon) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = GeoFunction::distance(polygon1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    double d = GeoFunction::distance(polygon1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    double d = GeoFunction::distance(polygon1, polygon2);
    EXPECT_EQ(314403.44451436587, d);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.25 0.25, 0.35 0.25, 0.35 0.35, 0.25 0.35, 0.25 0.25))")
            .value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = GeoFunction::distance(polygon1, polygon2);
    EXPECT_EQ(5559.651326278197, d);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-8.0 -8.0, -4.0 -8.0, -4.0 -4.0, -8.0 -4.0, -8.0 -8.0))")
            .value();
    double d = GeoFunction::distance(polygon1, polygon2);
    EXPECT_EQ(628758.784267869, d);
  }
}

TEST(s2CellIdFromPoint, point) {
  {
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    uint64_t i = GeoFunction::s2CellIdFromPoint(point);
    EXPECT_EQ(1153277837650709461, i);
  }
  {
    auto point = Geography::fromWKT("POINT(179.0 89.9)").value();
    uint64_t i = GeoFunction::s2CellIdFromPoint(point);
    EXPECT_EQ(6533220958669205147, i);
  }
  {
    auto point = Geography::fromWKT("POINT(-45.4 28.7652)").value();
    uint64_t i = GeoFunction::s2CellIdFromPoint(point);
    int64_t expectInt64 = -8444974090143026723;
    const char* c = reinterpret_cast<const char*>(&expectInt64);
    uint64_t expect = *reinterpret_cast<const uint64_t*>(c);
    EXPECT_EQ(expect, i);
  }
  {
    auto point = Geography::fromWKT("POINT(0.0 0.0)").value();
    uint64_t i = GeoFunction::s2CellIdFromPoint(point);
    EXPECT_EQ(1152921504606846977, i);
  }
}

// GeoFunction::s2CellIdFromPoint() just supports point
TEST(s2CellIdFromPoint, lineString) {
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    uint64_t i = GeoFunction::s2CellIdFromPoint(line);
    EXPECT_EQ(-1, i);
  }
}

// GeoFunction::s2CellIdFromPoint() just supports point
TEST(s2CellIdFromPoint, polygon) {
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    uint64_t i = GeoFunction::s2CellIdFromPoint(polygon);
    EXPECT_EQ(-1, i);
  }
}

TEST(isValid, point) {
  {
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = point.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto point = Geography::fromWKT("POINT(181.0 1.0)").value();
    bool b = point.isValid().ok();
    EXPECT_EQ(false, b);
  }
  {
    auto point = Geography::fromWKT("POINT(1.0 91.0)").value();
    bool b = point.isValid().ok();
    EXPECT_EQ(false, b);
  }
  {
    auto point = Geography::fromWKT("POINT(-181.0 -91.0)").value();
    bool b = point.isValid().ok();
    EXPECT_EQ(false, b);
  }
}

TEST(isValid, lineString) {
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = line.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1 1, 2 3, 4 8, -6 3)").value();
    bool b = line.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0)").value();
    bool b = line.isValid().ok();
    EXPECT_EQ(false, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 1.0 1.0)").value();
    bool b = line.isValid().ok();
    EXPECT_EQ(false, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 181.0 2.0)").value();
    bool b = line.isValid().ok();
    EXPECT_EQ(false, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 1.0 90.001)").value();
    bool b = line.isValid().ok();
    EXPECT_EQ(false, b);
  }
}

TEST(isValid, polygon) {
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT("POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20))").value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon =
        Geography::fromWKT(
            "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (10 0, 0 10, 0 -10, 10 0))")
            .value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT(
                       "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (10 0, 0 10, 0 -10, 10 "
                       "0), (-10 0, 0 10, -5 -10, -10 0))")
                       .value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT(
                       "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (1.0 1.0, 2.0 2.0, 0.0 "
                       "2.0, 1.0 1.0))")
                       .value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0))").value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(false, b);
  }
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.2 1.2))").value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(false, b);
  }
  // The first loop doesn't contain the second loop
  // {
  //   auto polygon = Geography::fromWKT(
  //                      "POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0), (-20 -20, -20 20, 20 20, 20
  //                      "
  //                      "-20, -20 -20))")
  //                      .value();
  //   bool b = polygon.isValid().ok();
  //   EXPECT_EQ(false, b);  // Expect false, got true
  // }
  {
    auto polygon =
        Geography::fromWKT("POLYGON((1.0 1.0, -180.0001 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(false, b);
  }
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 -90.001, 1.0 1.0))").value();
    bool b = polygon.isValid().ok();
    EXPECT_EQ(false, b);
  }
}

}  // namespace geo
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
