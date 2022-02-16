/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>      // for Future::get
#include <folly/init/Init.h>           // for init
#include <glog/logging.h>              // for INFO
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <stdint.h>                    // for int64_t
#include <thrift/lib/cpp2/FieldRef.h>  // for optional_field_ref

#include <cstddef>             // for size_t
#include <ext/alloc_traits.h>  // for __alloc_traits<>...
#include <limits>              // for numeric_limits
#include <memory>              // for allocator, uniqu...
#include <ostream>             // for operator<<
#include <string>              // for string, basic_st...
#include <type_traits>         // for remove_reference...
#include <unordered_map>       // for unordered_map
#include <utility>             // for pair, move, make...
#include <vector>              // for vector

#include "common/base/Base.h"                        // for kSrc, kDst, kRank
#include "common/base/Logging.h"                     // for Check_EQImpl, LOG
#include "common/base/ObjectPool.h"                  // for ObjectPool
#include "common/datatypes/DataSet.h"                // for DataSet
#include "common/datatypes/List.h"                   // for List
#include "common/datatypes/Value.h"                  // for Value
#include "common/expression/ConstantExpression.h"    // for ConstantExpression
#include "common/expression/Expression.h"            // for Expression
#include "common/expression/PropertyExpression.h"    // for EdgePropertyExpr...
#include "common/expression/RelationalExpression.h"  // for RelationalExpres...
#include "common/fs/TempDir.h"                       // for TempDir
#include "common/thrift/ThriftTypes.h"               // for PartitionID, Edg...
#include "interface/gen-cpp2/storage_types.h"        // for EdgeProp, ScanRe...
#include "mock/MockCluster.h"                        // for MockCluster
#include "mock/MockData.h"                           // for MockData, MockDa...
#include "storage/query/ScanEdgeProcessor.h"         // for ScanEdgeProcessor
#include "storage/test/QueryTestUtils.h"             // for QueryTestUtils

namespace nebula {
namespace storage {

cpp2::ScanEdgeRequest buildRequest(
    std::vector<PartitionID> partIds,
    std::vector<std::string> cursors,
    const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges,
    int64_t rowLimit = 100,
    int64_t startTime = 0,
    int64_t endTime = std::numeric_limits<int64_t>::max(),
    bool onlyLatestVer = false) {
  cpp2::ScanEdgeRequest req;
  req.space_id_ref() = 1;
  cpp2::ScanCursor c;
  CHECK_EQ(partIds.size(), cursors.size());
  std::unordered_map<PartitionID, cpp2::ScanCursor> parts;
  for (std::size_t i = 0; i < partIds.size(); ++i) {
    if (!cursors[i].empty()) {
      c.next_cursor_ref() = cursors[i];
    }
    parts.emplace(partIds[i], c);
  }
  req.parts_ref() = std::move(parts);
  std::vector<cpp2::EdgeProp> edgeProps;
  for (const auto& edge : edges) {
    EdgeType edgeType = edge.first;
    cpp2::EdgeProp edgeProp;
    edgeProp.type_ref() = edgeType;
    for (const auto& prop : edge.second) {
      (*edgeProp.props_ref()).emplace_back(std::move(prop));
    }
    edgeProps.emplace_back(std::move(edgeProp));
  }
  req.return_columns_ref() = std::move(edgeProps);
  req.limit_ref() = rowLimit;
  req.start_time_ref() = startTime;
  req.end_time_ref() = endTime;
  req.only_latest_version_ref() = onlyLatestVer;
  return req;
}

void checkResponse(const nebula::DataSet& dataSet,
                   const std::pair<EdgeType, std::vector<std::string>> edge,
                   size_t expectColumnCount,
                   size_t& totalRowCount) {
  ASSERT_EQ(dataSet.colNames.size(), expectColumnCount);
  if (!edge.second.empty()) {
    ASSERT_EQ(dataSet.colNames.size(), edge.second.size());
    for (size_t i = 0; i < dataSet.colNames.size(); i++) {
      ASSERT_EQ(dataSet.colNames[i], std::to_string(edge.first) + "." + edge.second[i]);
    }
  }
  totalRowCount += dataSet.rows.size();
  for (const auto& row : dataSet.rows) {
    // always pass name or kSrc at first
    auto srcId = row.values[0].getStr();
    auto edgeType = edge.first;
    ASSERT_EQ(expectColumnCount, row.values.size());
    auto props = edge.second;
    switch (edgeType) {
      case 101: {
        // out edge serve
        auto iter = mock::MockData::playerServes_.find(srcId);
        CHECK(iter != mock::MockData::playerServes_.end());
        QueryTestUtils::checkOutServe(edgeType, props, iter->second, row.values, 4, 5);
        break;
      }
      case -101: {
        // in edge teammates
        auto iter = mock::MockData::teamServes_.find(srcId);
        CHECK(iter != mock::MockData::teamServes_.end());
        std::vector<Value> values;
        QueryTestUtils::checkInServe(edgeType, props, iter->second, row.values);
        break;
      }
      default:
        LOG(FATAL) << "Should not reach here";
    }
  }
}

TEST(ScanEdgeTest, PropertyTest) {
  fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  EdgeType serve = 101;

  {
    LOG(INFO) << "Scan one edge with some properties in one batch";
    size_t totalRowCount = 0;
    auto edge = std::make_pair(
        serve,
        std::vector<std::string>{kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      auto req = buildRequest({partId}, {""}, {edge});
      auto* processor = ScanEdgeProcessor::instance(env, nullptr);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();

      ASSERT_EQ(0, resp.result.failed_parts.size());
      checkResponse(*resp.props_ref(), edge, edge.second.size(), totalRowCount);
    }
    CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
  }
  {
    LOG(INFO) << "Scan one edge with all properties in one batch";
    size_t totalRowCount = 0;
    auto edge = std::make_pair(serve, std::vector<std::string>{});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      auto req = buildRequest({partId}, {""}, {edge});
      auto* processor = ScanEdgeProcessor::instance(env, nullptr);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();

      ASSERT_EQ(0, resp.result.failed_parts.size());
      // all 9 columns in value
      checkResponse(*resp.props_ref(), edge, 9, totalRowCount);
    }
    CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
  }
}

TEST(ScanEdgeTest, CursorTest) {
  fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  EdgeType serve = 101;

  {
    LOG(INFO) << "Scan one edge with some properties with limit = 5";
    size_t totalRowCount = 0;
    auto edge = std::make_pair(
        serve,
        std::vector<std::string>{kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      bool hasNext = true;
      std::string cursor = "";
      while (hasNext) {
        auto req = buildRequest({partId}, {cursor}, {edge}, 5);
        auto* processor = ScanEdgeProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        checkResponse(*resp.props_ref(), edge, edge.second.size(), totalRowCount);
        hasNext = resp.get_cursors().at(partId).next_cursor_ref().has_value();
        if (hasNext) {
          CHECK(resp.get_cursors().at(partId).next_cursor_ref().has_value());
          cursor = *resp.get_cursors().at(partId).next_cursor_ref();
        }
      }
    }
    CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
  }
  {
    LOG(INFO) << "Scan one edge with some properties with limit = 1";
    size_t totalRowCount = 0;
    auto edge = std::make_pair(
        serve,
        std::vector<std::string>{kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      bool hasNext = true;
      std::string cursor = "";
      while (hasNext) {
        auto req = buildRequest({partId}, {cursor}, {edge}, 1);
        auto* processor = ScanEdgeProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        checkResponse(*resp.props_ref(), edge, edge.second.size(), totalRowCount);
        hasNext = resp.get_cursors().at(partId).next_cursor_ref().has_value();
        if (hasNext) {
          CHECK(resp.get_cursors().at(partId).next_cursor_ref().has_value());
          cursor = *resp.get_cursors().at(partId).next_cursor_ref();
        }
      }
    }
    CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
  }
}

TEST(ScanEdgeTest, MultiplePartsTest) {
  fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  EdgeType serve = 101;

  {
    LOG(INFO) << "Scan one edge with some properties in one batch";
    size_t totalRowCount = 0;
    auto edge = std::make_pair(
        serve,
        std::vector<std::string>{kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
    auto req = buildRequest({1, 3}, {"", ""}, {edge});
    auto* processor = ScanEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    checkResponse(*resp.props_ref(), edge, edge.second.size(), totalRowCount);
  }
  {
    LOG(INFO) << "Scan one edge with all properties in one batch";
    size_t totalRowCount = 0;
    auto edge = std::make_pair(serve, std::vector<std::string>{});
    auto req = buildRequest({1, 3}, {"", ""}, {edge});
    auto* processor = ScanEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    // all 9 columns in value
    checkResponse(*resp.props_ref(), edge, 9, totalRowCount);
  }
}

TEST(ScanEdgeTest, LimitTest) {
  fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  EdgeType serve = 101;

  {
    LOG(INFO) << "Scan one edge with some properties in one batch";
    constexpr std::size_t limit = 3;
    size_t totalRowCount = 0;
    auto edge = std::make_pair(
        serve,
        std::vector<std::string>{kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
    auto req = buildRequest({1}, {""}, {edge}, limit);
    auto* processor = ScanEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    checkResponse(*resp.props_ref(), edge, edge.second.size(), totalRowCount);
    EXPECT_EQ(totalRowCount, limit);
  }
  {
    LOG(INFO) << "Scan one edge with all properties in one batch";
    constexpr std::size_t limit = 3;
    size_t totalRowCount = 0;
    auto edge = std::make_pair(serve, std::vector<std::string>{});
    auto req = buildRequest({1}, {""}, {edge}, limit);
    auto* processor = ScanEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    // all 9 columns in value
    checkResponse(*resp.props_ref(), edge, 9, totalRowCount);
    EXPECT_EQ(totalRowCount, limit);
  }
}

TEST(ScanEdgeTest, FilterTest) {
  fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  EdgeType serve = 101;
  ObjectPool pool;

  {
    LOG(INFO) << "Scan one edge with some properties in one batch";
    constexpr std::size_t limit = 3;
    auto edge = std::make_pair(
        serve,
        std::vector<std::string>{kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
    auto req = buildRequest({1}, {""}, {edge}, limit);
    Expression* filter = EdgePropertyExpression::make(&pool, "101", kSrc);
    filter = RelationalExpression::makeEQ(
        &pool, filter, ConstantExpression::make(&pool, "Damian Lillard"));
    req.filter_ref() = filter->encode();
    auto* processor = ScanEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());

    DataSet expected({"101._src",
                      "101._type",
                      "101._rank",
                      "101._dst",
                      "101.teamName",
                      "101.startYear",
                      "101.endYear"});
    expected.emplace_back(
        List({"Damian Lillard", 101, 2012, "Trail Blazers", "Trail Blazers", 2012, 2020}));
    EXPECT_EQ(*resp.props_ref(), expected);
  }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
