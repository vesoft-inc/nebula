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

class WKTParserTest : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 protected:
  StatusOr<Geometry> parse(const std::string& wkt) {
    auto geomRet = WKTReader().read(wkt);
    NG_RETURN_IF_ERROR(geomRet);
    NG_RETURN_IF_ERROR(check(geomRet.value()));
    return geomRet;
  }

  Status check(const Geometry& geom) {
    auto wkt = WKTWriter().write(geom);
    auto geomCopyRet = WKTReader().read(wkt);
    auto geomCopy = geomCopyRet.value();
    auto wktCopy = WKTWriter().write(geomCopy);

    if (wkt != wktCopy) {
      return Status::Error("The reparsed geometry `%s' is different from origin `%s'.",
                           wktCopy.c_str(),
                           wkt.c_str());
    }
    return Status::OK();
  }
};

TEST_F(WKTParserTest, TestWKTParser) {
  // Point
  {
    std::string wkt = "POINT(24.7 36.842)";
    auto result = parse(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POINT(-179 36.842)";
    auto result = parse(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POINT(24.7, 36.842)";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `, 36.842'");
  }
  {
    std::string wkt = "POINT(179,";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `,'");
  }
  {
    std::string wkt = "POINT(-190 36.842)";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(),
              "SyntaxError: Longitude must be between -180 and 180 degrees near `-190'");
  }
  {
    std::string wkt = "POINT(179 91)";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(),
              "SyntaxError: Latitude must be between -90 and 90 degrees near `91'");
  }
  // LineString
  {
    std::string wkt = "LINESTRING(0 1, 1 2, 2 3, 3 4)";
    auto result = parse(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "LINESTRING(26.4 78.9, 138.725 52)";
    auto result = parse(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "LINESTRING(0 1, 2 3,)";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `)'");
  }
  {
    std::string wkt = "LINESTRING(0 1)";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(),
              "SyntaxError: LineString must have at least 2 coordinates near `0 1'");
  }
  // Polygon
  {
    std::string wkt = "POLYGON((0 1, 1 2, 2 3, 0 1))";
    auto result = parse(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POLYGON((0 1, 1 2, 2 3, 0 1), (4 5, 5 6, 6 7, 9 9, 4 5))";
    auto result = parse(wkt);
    ASSERT_TRUE(result.ok()) << result.status();
  }
  {
    std::string wkt = "POLYGON(0 1, 1 2, 2 3, 0 1)";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `0 1, 1 2'");
  }
  {
    std::string wkt = "POLYGON((0 1, 1 2, 0 1)";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "SyntaxError: syntax error near `)'");
  }
  {
    std::string wkt = "POLYGON((0 1, 1 2, 2 3, 3 4))";
    auto result = parse(wkt);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(),
              "SyntaxError: Polygon's LinearRing must be closed near `(0 1, 1 2, 2 3, 3 4)'");
  }
}

}  // namespace nebula
