/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/datatypes/Geography.h"
#include "common/geo/io/wkb/WKBReader.h"
#include "common/geo/io/wkb/WKBWriter.h"

namespace nebula {
namespace geo {

class WKBTest : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 protected:
  StatusOr<Geography> read(const Geography& geog) {
    auto wkb = WKBWriter().write(geog);
    auto geogRet = WKBReader().read(wkb);
    NG_RETURN_IF_ERROR(geogRet);
    NG_RETURN_IF_ERROR(check(geog, geogRet.value()));
    return geogRet;
  }

  Status check(const Geography& geog1, const Geography& geog2) {
    if (geog1 != geog2) {
      return Status::Error("The reparsed Geography is different from the origin.");
    }
    return Status::OK();
  }
};

TEST_F(WKBTest, TestWKB) {
  // Point
  {
    Point v(Coordinate(24.7, 36.842));
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(true, v.isValid().ok());
  }
  {
    Point v(Coordinate(-179, 36.842));
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(true, v.isValid().ok());
  }
  {
    Point v(Coordinate(24.7, 36.842));
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(true, v.isValid().ok());
  }
  {
    Point v(Coordinate(298.4, 499.99));
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(false, v.isValid().ok());
  }
  {
    Point v(Coordinate(24.7, 36.842));
    auto wkb = WKBWriter().write(v);
    wkb.pop_back();
    auto geogRet = WKBReader().read(wkb);
    ASSERT_FALSE(geogRet.ok());
    EXPECT_EQ("Unexpected EOF when reading double", geogRet.status().toString());
  }
  // LineString
  {
    LineString v(std::vector<Coordinate>{
        Coordinate(0, 1), Coordinate(1, 2), Coordinate(2, 3), Coordinate(3, 4)});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(true, v.isValid().ok());
  }
  {
    LineString v(std::vector<Coordinate>{Coordinate(26.4, 78.9), Coordinate(138.725, 91.0)});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(false, v.isValid().ok());
  }
  {
    LineString v(std::vector<Coordinate>{Coordinate(0, 1), Coordinate(2, 3), Coordinate(0, 1)});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(true, v.isValid().ok());
  }
  {
    LineString v(std::vector<Coordinate>{Coordinate(0, 1), Coordinate(0, 1)});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(false, v.isValid().ok());
  }
  // LineString must have at least 2 points
  {
    LineString v(std::vector<Coordinate>{Coordinate(0, 1)});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(false, v.isValid().ok());
  }
  {
    LineString v(std::vector<Coordinate>{Coordinate(26.4, 78.9), Coordinate(138.725, 91.0)});
    auto wkb = WKBWriter().write(v);
    wkb.pop_back();
    auto geogRet = WKBReader().read(wkb);
    ASSERT_FALSE(geogRet.ok());
    EXPECT_EQ("Unexpected EOF when reading double", geogRet.status().toString());
  }
  {
    LineString v(std::vector<Coordinate>{Coordinate(26.4, 78.9), Coordinate(138.725, 91.0)});
    auto wkb = WKBWriter().write(v);
    wkb.erase(sizeof(uint8_t) + sizeof(uint32_t) + 3);  // Now the numCoord field is missing 1 byte
    auto geogRet = WKBReader().read(wkb);
    ASSERT_FALSE(geogRet.ok());
    EXPECT_EQ("Unexpected EOF when reading uint32_t", geogRet.status().toString());
  }
  // Polygon
  {
    Polygon v(std::vector<std::vector<Coordinate>>{std::vector<Coordinate>{
        Coordinate(0, 1), Coordinate(1, 2), Coordinate(2, 3), Coordinate(0, 1)}});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(true, v.isValid().ok());
  }
  {
    Polygon v(std::vector<std::vector<Coordinate>>{
        std::vector<Coordinate>{
            Coordinate(0, 1), Coordinate(1, 2), Coordinate(2, 3), Coordinate(0, 1)},
        std::vector<Coordinate>{Coordinate(4, 5),
                                Coordinate(5, 6),
                                Coordinate(6, 7),
                                Coordinate(9, 9),
                                Coordinate(4, 5)}});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(true, v.isValid().ok());
  }
  // The loop is not closed
  {
    Polygon v(std::vector<std::vector<Coordinate>>{std::vector<Coordinate>{
        Coordinate(0, 1), Coordinate(1, 2), Coordinate(2, 3), Coordinate(3, 4)}});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(false, v.isValid().ok());
  }
  // Loop must have at least 4 points
  {
    Polygon v(std::vector<std::vector<Coordinate>>{
        std::vector<Coordinate>{Coordinate(0, 1), Coordinate(1, 2), Coordinate(0, 1)}});
    auto result = read(v);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(false, v.isValid().ok());
  }
  {
    Polygon v(std::vector<std::vector<Coordinate>>{std::vector<Coordinate>{
        Coordinate(0, 1), Coordinate(1, 2), Coordinate(2, 3), Coordinate(0, 1)}});
    auto wkb = WKBWriter().write(v);
    wkb.pop_back();
    auto geogRet = WKBReader().read(wkb);
    ASSERT_FALSE(geogRet.ok());
    EXPECT_EQ("Unexpected EOF when reading double", geogRet.status().toString());
  }
  {
    Polygon v(std::vector<std::vector<Coordinate>>{std::vector<Coordinate>{
        Coordinate(0, 1), Coordinate(1, 2), Coordinate(2, 3), Coordinate(0, 1)}});
    auto wkb = WKBWriter().write(v);
    wkb.erase(sizeof(uint8_t) + sizeof(uint32_t) +
              3);  // Now the numCoordList field is missing 1 byte
    auto geogRet = WKBReader().read(wkb);
    ASSERT_FALSE(geogRet.ok());
    EXPECT_EQ("Unexpected EOF when reading uint32_t", geogRet.status().toString());
  }
}

}  // namespace geo
}  // namespace nebula
