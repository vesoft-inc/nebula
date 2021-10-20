/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/geo/io/wkt/WKTReader.h"
#include "common/geo/io/wkt/WKTWriter.h"

namespace nebula {
namespace geo {

class WKTTEST : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 protected:
  StatusOr<Geography> read(const std::string& wkt) {
    auto geogRet = WKTReader().read(wkt);
    NG_RETURN_IF_ERROR(geogRet);
    NG_RETURN_IF_ERROR(check(geogRet.value()));
    return geogRet;
  }

  Status check(const Geography& geog) {
    auto wkt = WKTWriter().write(geog);
    auto geogCopyRet = WKTReader().read(wkt);
    auto geogCopy = geogCopyRet.value();
    auto wktCopy = WKTWriter().write(geogCopy);

    if (wkt != wktCopy) {
      return Status::Error("The reparsed Geography `%s' is different from origin `%s'.",
                           wktCopy.c_str(),
                           wkt.c_str());
    }
    return Status::OK();
  }
};

TEST_F(WKTTEST, TestWKT) {
  // Point
  {
    std::string wkt = "POINT(24.7 36.842)";
    auto result = read(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POINT(-179 36.842)";
    auto result = read(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POINT(24.7, 36.842)";
    auto result = read(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `, 36.842'");
  }
  {
    std::string wkt = "POINT(179,";
    auto result = read(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `,'");
  }
  // LineString
  {
    std::string wkt = "LINESTRING(0 1, 1 2, 2 3, 3 4)";
    auto result = read(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "LINESTRING(26.4 78.9, 138.725 52)";
    auto result = read(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "LINESTRING(0 1, 2 3,)";
    auto result = read(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `)'");
  }
  // Polygon
  {
    std::string wkt = "POLYGON((0 1, 1 2, 2 3, 0 1))";
    auto result = read(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POLYGON((0 1, 1 2, 2 3, 0 1), (4 5, 5 6, 6 7, 9 9, 4 5))";
    auto result = read(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POLYGON(0 1, 1 2, 2 3, 0 1)";
    auto result = read(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `0 1, 1 2'");
  }
  {
    std::string wkt = "POLYGON((0 1, 1 2, 0 1)";
    auto result = read(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `)'");
  }
}

}  // namespace geo
}  // namespace nebula
