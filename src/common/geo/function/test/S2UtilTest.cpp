/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/geo/function/S2Util.h"

namespace nebula {

TEST(s2CellIdFromPoint, point) {
  {
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    uint64_t i = s2CellIdFromPoint(point);
    EXPECT_EQ(1153277837650709461, i);
  }
  {
    auto point = Geography::fromWKT("POINT(179.0 89.9)").value();
    uint64_t i = s2CellIdFromPoint(point);
    EXPECT_EQ(6533220958669205147, i);
  }
  {
    auto point = Geography::fromWKT("POINT(-45.4 28.7652)").value();
    uint64_t i = s2CellIdFromPoint(point);
    int64_t expectInt64 = -8444974090143026723;
    const char* c = reinterpret_cast<const char*>(&expectInt64);
    uint64_t expect = *reinterpret_cast<const uint64_t*>(c);
    EXPECT_EQ(expect, i);
  }
  {
    auto point = Geography::fromWKT("POINT(0.0 0.0)").value();
    uint64_t i = s2CellIdFromPoint(point);
    EXPECT_EQ(1152921504606846977, i);
  }
}

// s2CellIdFromPoint() just supports point
TEST(s2CellIdFromPoint, lineString) {
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    uint64_t i = s2CellIdFromPoint(line);
    EXPECT_EQ(-1, i);
  }
}

// s2CellIdFromPoint() just supports point
TEST(s2CellIdFromPoint, polygon) {
  {
    auto polygon = Geography::fromWKT("POLYGON((1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))").value();
    uint64_t i = s2CellIdFromPoint(polygon);
    EXPECT_EQ(-1, i);
  }
}

TEST(coveringCellIds, point) {
  {
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    std::vector<uint64_t> cellIds = s2CoveringCellIds(point);
    EXPECT_EQ(std::vector<uint64_t>{1153277837650709461}, cellIds);
  }
  {
    auto point = Geography::fromWKT("POINT(179.0 89.9)").value();
    std::vector<uint64_t> cellIds = s2CoveringCellIds(point);
    EXPECT_EQ(std::vector<uint64_t>{6533220958669205147}, cellIds);
  }
  {
    auto point = Geography::fromWKT("POINT(-45.4 28.7652)").value();
    std::vector<uint64_t> cellIds = s2CoveringCellIds(point);
    int64_t expectInt64 = -8444974090143026723;
    const char* c = reinterpret_cast<const char*>(&expectInt64);
    uint64_t expect = *reinterpret_cast<const uint64_t*>(c);
    EXPECT_EQ(std::vector<uint64_t>{expect}, cellIds);
  }
  {
    auto point = Geography::fromWKT("POINT(0.0 0.0)").value();
    std::vector<uint64_t> cellIds = s2CoveringCellIds(point);
    EXPECT_EQ(std::vector<uint64_t>{1152921504606846977}, cellIds);
  }
}

TEST(coveringCellIds, linestring) {
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    std::vector<uint64_t> cellIds = s2CoveringCellIds(line);
    EXPECT_EQ(std::vector<uint64_t>{1153277837650709461}, cellIds);
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
