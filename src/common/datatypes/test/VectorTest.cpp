/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/datatypes/Vector.h"

TEST(VectorTest, Basic) {
  nebula::Vector shortVec1({1.11, 2.22, 3.33});
  nebula::Vector emptyVec;
  // we will test dimension in ValueTest.cpp
  // so here we assume the dimensions of two vectors are equal

  EXPECT_EQ(shortVec1.dim(), 3);
  EXPECT_EQ(emptyVec.dim(), 0);
  EXPECT_EQ(shortVec1, nebula::Vector({1.11, 2.22, 3.33}));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
