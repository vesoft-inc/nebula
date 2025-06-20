/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/datatypes/Vector.h"

TEST(VectorTest, Basic) {
  nebula::Vector shortVec1({1.11, 2.22, 3.33}), shortVec2({4.44, 5.55, 6.66});
  nebula::Vector longVec1({1.11, 2.22, 3.33, 4.44, 5.55, 6.66}),
      longVec2({7.77, 8.88, 9.99, 10.10, 11.11, 12.12});
  nebula::Vector emptyVec;
  // we will test dimension in ValueTest.cpp
  // so here we assume the dimensions of two vectors are equal

  EXPECT_EQ(shortVec1.dim(), 3);
  EXPECT_EQ(emptyVec.dim(), 0);
  EXPECT_EQ(shortVec1, nebula::Vector({1.11, 2.22, 3.33}));
  nebula::Vector sumVec = shortVec1 + shortVec2;
  bool v = sumVec == nebula::Vector({5.55, 7.77, 9.99});
  EXPECT_EQ(v, true);
  sumVec = longVec1 + longVec2;
  v = sumVec == nebula::Vector({8.88, 11.10, 13.32, 14.54, 16.66, 18.78});
  EXPECT_EQ(v, true);
  sumVec = shortVec1 - shortVec2;
  v = sumVec == nebula::Vector({-3.33, -3.33, -3.33});
  EXPECT_EQ(v, true);
  sumVec = longVec1 - longVec2;
  v = sumVec == nebula::Vector({-6.66, -6.66, -6.66, -5.66, -5.56, -5.46});
  EXPECT_EQ(v, true);
  v = shortVec1 < shortVec2;
  EXPECT_EQ(v, true);
  v = shortVec1 > shortVec2;
  EXPECT_EQ(v, false);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
