/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"

namespace nebula {

TEST(isValid, point) {
  {
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    bool b = point.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto point = Geography::fromWKT("POINT(181.0 1.0)").value();
    bool b = point.isValid();
    EXPECT_EQ(false, b);
  }
  {
    auto point = Geography::fromWKT("POINT(1.0 91.0)").value();
    bool b = point.isValid();
    EXPECT_EQ(false, b);
  }
  {
    auto point = Geography::fromWKT("POINT(-181.0 -91.0)").value();
    bool b = point.isValid();
    EXPECT_EQ(false, b);
  }
}

TEST(isValid, lineString) {
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    bool b = line.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1 1, 2 3, 4 8, -6 3)").value();
    bool b = line.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0)").value();
    bool b = line.isValid();
    EXPECT_EQ(false, b);
  }
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 1.0 1.0)").value();
    bool b = line.isValid();
    EXPECT_EQ(false, b);
  }
}

TEST(isValid, polygon) {
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    bool b = polygon.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT("POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20))").value();
    bool b = polygon.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon =
        Geography::fromWKT(
            "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (10 0, 0 10, 0 -10, 10 0))")
            .value();
    bool b = polygon.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT(
                       "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (10 0, 0 10, 0 -10, 10 "
                       "0), (-10 0, 0 10, -5 -10, -10 0))")
                       .value();
    bool b = polygon.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT(
                       "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (1.0 1.0, 2.0 2.0, 0.0 "
                       "2.0, 1.0 1.0))")
                       .value();
    bool b = polygon.isValid();
    EXPECT_EQ(true, b);
  }
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0))").value();
    bool b = polygon.isValid();
    EXPECT_EQ(false, b);
  }
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.2 1.2))").value();
    bool b = polygon.isValid();
    EXPECT_EQ(false, b);
  }
  // The first loop doesn't contain the second loop
  {
    auto polygon = Geography::fromWKT(
                       "POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0), (-20 -20, -20 20, 20 20, 20 "
                       "-20, -20 -20))")
                       .value();
    bool b = polygon.isValid();
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
