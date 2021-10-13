/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <s2/util/coding/coder.h>

#include "common/base/Base.h"
#include "common/datatypes/Geography.h"

namespace nebula {

TEST(Geography, shape) {
  {
    std::string wkt = "POINT(3 7)";
    auto gRet = Geography::fromWKT(wkt);
    ASSERT_TRUE(gRet.ok());
    auto g = gRet.value();
    EXPECT_EQ(GeoShape::POINT, g.shape());
  }
  {
    std::string wkt = "LINESTRING(28.4 79.20,134.25 -28.34)";
    auto gRet = Geography::fromWKT(wkt);
    ASSERT_TRUE(gRet.ok());
    auto g = gRet.value();
    EXPECT_EQ(GeoShape::LINESTRING, g.shape());
  }
  {
    std::string wkt = "POLYGON((1 2,3 4,5 6,1 2))";
    auto gRet = Geography::fromWKT(wkt);
    ASSERT_TRUE(gRet.ok());
    auto g = gRet.value();
    EXPECT_EQ(GeoShape::POLYGON, g.shape());
  }
}

TEST(Geography, asWKT) {
  {
    std::string wkt = "POINT(3 7)";
    auto gRet = Geography::fromWKT(wkt);
    ASSERT_TRUE(gRet.ok());
    auto g = gRet.value();
    std::string got = g.asWKT();
    EXPECT_EQ(wkt, got);
  }
  {
    std::string wkt = "LINESTRING(28.4 79.2,134.25 -28.34)";
    auto gRet = Geography::fromWKT(wkt);
    ASSERT_TRUE(gRet.ok());
    auto g = gRet.value();
    std::string got = g.asWKT();
    EXPECT_EQ(wkt, got);
  }
  {
    std::string wkt = "POLYGON((1 2,3 4,5 6,1 2))";
    auto gRet = Geography::fromWKT(wkt);
    ASSERT_TRUE(gRet.ok());
    auto g = gRet.value();
    std::string got = g.asWKT();
    EXPECT_EQ(wkt, got);
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
