/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/geo/function/DWithin.h"

namespace nebula {

TEST(DWithin, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = dWithin(point1, point2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = dWithin(point1, point2, 0.0, false);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = dWithin(point1, point2, 0.1, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = dWithin(point1, point2, 22239.020235496788, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = dWithin(point1, point2, 22239.020235496788, false);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = dWithin(point1, point2, 22240, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(-118.4079 33.9434)").value();
    auto point2 = Geography::fromWKT("POINT(2.5559 49.0083)").value();
    bool b = dWithin(point1, point2, 10000, false);
    EXPECT_EQ(false, b);
  }
}

TEST(DWithin, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = dWithin(point1, line2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = dWithin(point1, line2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.2, 1.5 1.6)").value();
    bool b = dWithin(point1, line2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.2 3.8)").value();
    auto point2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = dWithin(point1, point2, 1.0, false);
    EXPECT_EQ(false, b);
  }
}

TEST(DWithin, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = dWithin(point1, polygon2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  // Point lies on the interior of the polygon
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.9)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = dWithin(point1, polygon2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = dWithin(point1, polygon2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = dWithin(point1, polygon2, 99999, false);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.3 0.3)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = dWithin(point1, polygon2, 10000, true);
    EXPECT_EQ(false, b);
  }
}

TEST(DWithin, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = dWithin(line1, line2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = dWithin(line1, line2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.0 0.0, 1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(5.0 5.0, 6.0 6.0)").value();
    bool b = dWithin(line1, line2, 628519, true);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 2.0 1.0)").value();
    bool b = dWithin(line1, line2, 1.0, false);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 1.0 4.0)").value();
    bool b = dWithin(line1, line2, 78609, false);
    EXPECT_EQ(true, b);
  }
}

TEST(DWithin, lineString2Polygon) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = dWithin(line1, polygon2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.2 0.2, 0.4 0.4)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = dWithin(line1, polygon2, 0.0, false);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = dWithin(line1, polygon2, 9999, false);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.35 0.35)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = dWithin(line1, polygon2, 5555, true);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.25 0.25, 0.6 0.6)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = dWithin(line1, polygon2, 0.1, false);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(7.0 7.0, 5.0 5.0)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = dWithin(line1, polygon2, 628520, false);
    EXPECT_EQ(true, b);
  }
}

TEST(DWithin, polygon2Polygon) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = dWithin(polygon1, polygon2, 0.0, true);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    bool b = dWithin(polygon1, polygon2, 1.1, false);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    bool b = dWithin(polygon1, polygon2, 314402, true);
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
    bool b = dWithin(polygon1, polygon2, 5560, false);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-8.0 -8.0, -4.0 -8.0, -4.0 -4.0, -8.0 -4.0, -8.0 -8.0))")
            .value();
    bool b = dWithin(polygon1, polygon2, 628750, false);
    EXPECT_EQ(false, b);
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
