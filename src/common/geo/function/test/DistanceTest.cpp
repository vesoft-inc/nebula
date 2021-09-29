/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/geo/function/Distance.h"

namespace nebula {

TEST(Distance, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    double d = distance(point1, point2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    double d = distance(point1, point2);
    EXPECT_EQ(22239.020235496788, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.00001)").value();
    double d = distance(point1, point2);
    EXPECT_EQ(1.1119510117584994, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(-118.4079 33.9434)").value();
    auto point2 = Geography::fromWKT("POINT(2.5559 49.0083)").value();
    double d = distance(point1, point2);
    EXPECT_EQ(9103089.738448681, d);
  }
}

TEST(Distance, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    double d = distance(point1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    double d = distance(point1, line2);
    EXPECT_EQ(0.0, d);  // 0.000000000031290120680325526
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.2, 1.5 1.6)").value();
    double d = distance(point1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.2 3.8)").value();
    auto point2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    double d = distance(point1, point2);
    EXPECT_EQ(198856.0525640426, d);
  }
}

TEST(Distance, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    double d = distance(point1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  // Point lies on the interior of the polygon
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.9)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    double d = distance(point1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = distance(point1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = distance(point1, polygon2);
    EXPECT_EQ(86836.82688844757, d);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.3 0.3)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = distance(point1, polygon2);
    EXPECT_EQ(11119.357694100196, d);
  }
}

TEST(Distance, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    double d = distance(line1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    double d = distance(line1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.0 0.0, 1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(5.0 5.0, 6.0 6.0)").value();
    double d = distance(line1, line2);
    EXPECT_EQ(628519.1549911008, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 2.0 1.0)").value();
    double d = distance(line1, line2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 1.0 4.0)").value();
    double d = distance(line1, line2);
    EXPECT_EQ(78608.33038471815, d);
  }
}

TEST(Distance, lineString2Polygon) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.2 0.2, 0.4 0.4)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.35 0.35)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = distance(line1, polygon2);
    EXPECT_EQ(5559.651326278197, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.6 0.6)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = distance(line1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(7.0 7.0, 5.0 5.0)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    double d = distance(line1, polygon2);
    EXPECT_EQ(628519.1549911008, d);
  }
}

TEST(Distance, polygon2Polygon) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    double d = distance(polygon1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    double d = distance(polygon1, polygon2);
    EXPECT_EQ(0.0, d);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    double d = distance(polygon1, polygon2);
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
    double d = distance(polygon1, polygon2);
    EXPECT_EQ(5559.651326278197, d);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-8.0 -8.0, -4.0 -8.0, -4.0 -4.0, -8.0 -4.0, -8.0 -8.0))")
            .value();
    double d = distance(polygon1, polygon2);
    EXPECT_EQ(628758.784267869, d);
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
