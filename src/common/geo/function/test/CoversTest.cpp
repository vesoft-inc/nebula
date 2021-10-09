/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/geo/function/Covers.h"

namespace nebula {

TEST(Covers, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = covers(point1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = covers(point1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.00000000001)").value();
    bool b = covers(point1, point2);
    EXPECT_EQ(false, b);
  }
  // {
  //   auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
  //   auto point2 = Geography::fromWKT("POINT(1.0 1.000000000001)").value();
  //   bool b = covers(point1, point2);
  //   EXPECT_EQ(true, b); // The error should be 1e-11?
  // }
}

TEST(Covers, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = covers(point1, line2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = covers(point1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, lineString2Point) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = covers(line1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(2.0 2.0)").value();
    bool b = covers(line1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(3.0 3.0)").value();
    bool b = covers(line1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(2.0 4.0)").value();
    bool b = covers(line1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.5 1.5001714)").value();
    bool b = covers(line1, point2);
    EXPECT_EQ(true, b);  // false
  }
}

TEST(Covers, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    bool b = covers(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(4.0 4.0, 3.0 3.0, 2.0 2.0)").value();
    bool b = covers(line1, line2);
    EXPECT_EQ(true, b);  // Line should covers its reverse
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    bool b = covers(line1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    bool b = covers(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(5.0 5.0, 4.0 4.0)").value();
    bool b = covers(line1, line2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, lineString2Polygon) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = covers(line1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(Covers, polygon2Point) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto point2 = Geography::fromWKT("POINT(0.5 0.5)").value();
    bool b = covers(polygon1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto point2 = Geography::fromWKT("POINT(0.5 7.7)").value();
    bool b = covers(polygon1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto point2 = Geography::fromWKT("POINT(0.3 0.3)").value();
    bool b = covers(polygon1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto point2 = Geography::fromWKT("POINT(1.0 0.0)").value();
    bool b = covers(polygon1, point2);
    EXPECT_EQ(true, b);
  }
}

TEST(Covers, polygon2LineString) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(0.1 0.1, 0.4 0.4)").value();
    bool b = covers(polygon1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto line2 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.35 0.35)").value();
    bool b = covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto line2 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.55 0.55)").value();
    bool b = covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    auto line2 = Geography::fromWKT("LINESTRING(0.3 0.3, 4.0 4.0)").value();
    bool b = covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(-1 -1, -2 -2)").value();
    bool b = covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(-0.5 -0.5, 0.5 0.5)").value();
    bool b = covers(polygon1, line2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(0.0 0.0, 1.0 0.0)").value();
    bool b = covers(polygon1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 0.0, 0.0 0.0)").value();
    bool b = covers(
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
    bool b = covers(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    bool b = covers(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))").value();
    bool b = covers(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))").value();
    bool b = covers(polygon1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    bool b = covers(polygon1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(CoveredBy, basic) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = coveredBy(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0, 4.0 4.0)").value();
    bool b = coveredBy(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = coveredBy(line1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 1.0, 1.5 1.5, 0.0 1.0, 0.0 0.0))").value();
    bool b = coveredBy(line1, polygon2);
    EXPECT_EQ(true, b);
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
