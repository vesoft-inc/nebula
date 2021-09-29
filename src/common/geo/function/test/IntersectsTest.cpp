/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/geo/function/Intersects.h"

namespace nebula {

TEST(Intersects, point2Point) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = intersects(point1, point2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.2)").value();
    bool b = intersects(point1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.00000000001)").value();
    bool b = intersects(point1, point2);
    EXPECT_EQ(false, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto point2 = Geography::fromWKT("POINT(1.0 1.000000000001)").value();
    bool b = intersects(point1, point2);
    EXPECT_EQ(true, b);
  }
}

TEST(Intersects, point2LineString) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = intersects(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(3.0 3.000000000001, 2.0 2.0)").value();
    bool b = intersects(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.6)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = intersects(point1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.2 3.8)").value();
    auto point2 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.6, 2.0 2.2)").value();
    bool b = intersects(point1, point2);
    EXPECT_EQ(false, b);
  }
}

TEST(Intersects, point2Polygon) {
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = intersects(point1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.0 1.000000000001)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = intersects(point1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(1.5 1.9)").value();
    auto polygon2 = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = intersects(point1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto point1 = Geography::fromWKT("POINT(0.3 0.3)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = intersects(point1, polygon2);
    EXPECT_EQ(false, b);
  }
}

TEST(Intersects, lineString2LineString) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(2.0 2.0, 3.0 3.0)").value();
    bool b = intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.5499860 1.5501575, 1.5 1.5001714)").value();
    bool b = intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 2.0 1.0)").value();
    bool b = intersects(line1, line2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto line2 = Geography::fromWKT("LINESTRING(1.0 2.0, 1.0 4.0)").value();
    bool b = intersects(line1, line2);
    EXPECT_EQ(false, b);
  }
}

TEST(Intersects, lineString2Polygon) {
  {
    auto line1 = Geography::fromWKT("LINESTRING(1.0 1.0, 1.5 1.5, 2.0 2.0)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.2 0.2, 0.4 0.4)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 0.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, 1.5 0.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(-0.5 0.5, -1.5 -1.5)").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.35 0.35)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(false, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.3 0.3, 0.5 0.5)").value();
    auto polygon2 = Geography::fromWKT(
                        "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.2 0.2, 0.2 0.4, "
                        "0.4 0.4, 0.4 0.2, 0.2 0.2))")
                        .value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto line1 = Geography::fromWKT("LINESTRING(0.4 0.3, 0.6 0.3)").value();
    auto polygon2 =
        Geography::fromWKT(
            "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0), (0.3 0.3, 0.4 0.2, 0.5 0.3, "
            "0.4 0.4, 0.3 0.3), (0.5 0.3, 0.6 0.2, 0.7 0.3, 0.6 0.4, 0.5 0.3))")
            .value();
    bool b = intersects(line1, polygon2);
    EXPECT_EQ(true, b);
  }
}

TEST(Intersects, polygon2Polygon) {
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    bool b = intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.2 0.2, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.2 0.2))").value();
    bool b = intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }
  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))").value();
    bool b = intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }

  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((-1.0 0.0, 1.0 0.0, 1.0 1.0, -1.0 1.0, -1.0 0.0))").value();
    bool b = intersects(polygon1, polygon2);
    EXPECT_EQ(true, b);
  }

  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((3.0 3.0, 4.0 3.0, 4.0 4.0, 3.0 4.0, 3.0 3.0))").value();
    bool b = intersects(polygon1, polygon2);
    EXPECT_EQ(false, b);
  }

  {
    auto polygon1 =
        Geography::fromWKT("POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))").value();
    auto polygon2 =
        Geography::fromWKT("POLYGON((0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))").value();
    bool b = intersects(polygon1, polygon2);
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
