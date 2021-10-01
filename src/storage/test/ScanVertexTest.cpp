/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <bits/c++config.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "storage/query/ScanVertexProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

cpp2::ScanVertexRequest buildRequest(
    std::vector<PartitionID> partIds,
    std::vector<std::string> cursors,
    const std::vector<std::pair<TagID, std::vector<std::string>>>& tags,
    int64_t rowLimit = 100,
    int64_t startTime = 0,
    int64_t endTime = std::numeric_limits<int64_t>::max(),
    bool onlyLatestVer = false) {
  cpp2::ScanVertexRequest req;
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
  std::vector<cpp2::VertexProp> vertexProps;
  for (const auto& tag : tags) {
    TagID tagId = tag.first;
    cpp2::VertexProp vertexProp;
    vertexProp.tag_ref() = tagId;
    for (const auto& prop : tag.second) {
      (*vertexProp.props_ref()).emplace_back(std::move(prop));
    }
    vertexProps.emplace_back(std::move(vertexProp));
  }
  req.return_columns_ref() = std::move(vertexProps);
  req.limit_ref() = rowLimit;
  req.start_time_ref() = startTime;
  req.end_time_ref() = endTime;
  req.only_latest_version_ref() = onlyLatestVer;
  return req;
}

void checkResponse(const nebula::DataSet& dataSet,
                   const std::pair<TagID, std::vector<std::string>>& tag,
                   size_t expectColumnCount,
                   size_t& totalRowCount) {
  ASSERT_EQ(dataSet.colNames.size(), expectColumnCount);
  if (!tag.second.empty()) {
    ASSERT_EQ(dataSet.colNames.size(), tag.second.size() + 1 /* kVid*/);
    for (size_t i = 0; i < dataSet.colNames.size() - 1 /* kVid */; i++) {
      ASSERT_EQ(dataSet.colNames[i + 1 /* kVid */],
                std::to_string(tag.first) + "." + tag.second[i]);
    }
  }
  totalRowCount += dataSet.rows.size();
  for (const auto& row : dataSet.rows) {
    // always pass player name or kVid at first
    auto vId = row.values[0].getStr();
    auto tagId = tag.first;
    ASSERT_EQ(expectColumnCount, row.values.size());
    auto props = tag.second;
    switch (tagId) {
      case 1: {
        // tag player
        auto iter = std::find_if(mock::MockData::players_.begin(),
                                 mock::MockData::players_.end(),
                                 [&](const auto& player) { return player.name_ == vId; });
        CHECK(iter != mock::MockData::players_.end());
        std::vector<std::string> returnProps({kVid});
        returnProps.insert(returnProps.end(), props.begin(), props.end());
        QueryTestUtils::checkPlayer(returnProps, *iter, row.values);
        break;
      }
      case 2: {
        // tag team
        auto iter = std::find(mock::MockData::teams_.begin(), mock::MockData::teams_.end(), vId);
        std::vector<std::string> returnProps({kVid});
        returnProps.insert(returnProps.end(), props.begin(), props.end());
        QueryTestUtils::checkTeam(returnProps, *iter, row.values);
        break;
      }
      default:
        LOG(FATAL) << "Should not reach here";
    }
  }
}

TEST(ScanVertexTest, PropertyTest) {
  fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  TagID player = 1;

  {
    LOG(INFO) << "Scan one tag with some properties in one batch";
    size_t totalRowCount = 0;
    auto tag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      auto req = buildRequest({partId}, {""}, {tag});
      auto* processor = ScanVertexProcessor::instance(env, nullptr);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();

      ASSERT_EQ(0, resp.result.failed_parts.size());
      checkResponse(*resp.props_ref(), tag, tag.second.size() + 1 /* kVid */, totalRowCount);
    }
    CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
  }
  {
    LOG(INFO) << "Scan one tag with all properties in one batch";
    size_t totalRowCount = 0;
    auto tag = std::make_pair(player, std::vector<std::string>{});
    auto respTag = std::make_pair(player,
                                  std::vector<std::string>{"name",
                                                           "age",
                                                           "playing",
                                                           "career",
                                                           "startYear",
                                                           "endYear",
                                                           "games",
                                                           "avgScore",
                                                           "serveTeams",
                                                           "country",
                                                           "champions"});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      auto req = buildRequest({partId}, {""}, {tag});
      auto* processor = ScanVertexProcessor::instance(env, nullptr);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();

      ASSERT_EQ(0, resp.result.failed_parts.size());
      // all 11 columns in value
      checkResponse(*resp.props_ref(), respTag, 11 + 1 /* kVid */, totalRowCount);
    }
    CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
  }
}

TEST(ScanVertexTest, CursorTest) {
  fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  TagID player = 1;

  {
    LOG(INFO) << "Scan one tag with some properties with limit = 5";
    size_t totalRowCount = 0;
    auto tag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      bool hasNext = true;
      std::string cursor = "";
      while (hasNext) {
        auto req = buildRequest({partId}, {cursor}, {tag}, 5);
        auto* processor = ScanVertexProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        checkResponse(*resp.props_ref(), tag, tag.second.size() + 1 /* kVid */, totalRowCount);
        hasNext = resp.get_cursors().at(partId).next_cursor_ref().has_value();
        if (hasNext) {
          CHECK(resp.get_cursors().at(partId).next_cursor_ref());
          cursor = *resp.get_cursors().at(partId).next_cursor_ref();
        }
      }
    }
    CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
  }
  {
    LOG(INFO) << "Scan one tag with some properties with limit = 1";
    size_t totalRowCount = 0;
    auto tag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    for (PartitionID partId = 1; partId <= totalParts; partId++) {
      bool hasNext = true;
      std::string cursor = "";
      while (hasNext) {
        auto req = buildRequest({partId}, {cursor}, {tag}, 1);
        auto* processor = ScanVertexProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        checkResponse(*resp.props_ref(), tag, tag.second.size() + 1 /* kVid */, totalRowCount);
        hasNext = resp.get_cursors().at(partId).next_cursor_ref().has_value();
        if (hasNext) {
          CHECK(resp.get_cursors().at(partId).next_cursor_ref());
          cursor = *resp.get_cursors().at(partId).next_cursor_ref();
        }
      }
    }
    CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
  }
}

TEST(ScanVertexTest, MultiplePartsTest) {
  fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  TagID player = 1;

  {
    LOG(INFO) << "Scan one tag with some properties in one batch";
    size_t totalRowCount = 0;
    auto tag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    auto req = buildRequest({1, 3}, {"", ""}, {tag});
    auto* processor = ScanVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    checkResponse(*resp.props_ref(), tag, tag.second.size() + 1 /* kVid */, totalRowCount);
  }
  {
    LOG(INFO) << "Scan one tag with all properties in one batch";
    size_t totalRowCount = 0;
    auto tag = std::make_pair(player, std::vector<std::string>{});
    auto respTag = std::make_pair(player,
                                  std::vector<std::string>{"name",
                                                           "age",
                                                           "playing",
                                                           "career",
                                                           "startYear",
                                                           "endYear",
                                                           "games",
                                                           "avgScore",
                                                           "serveTeams",
                                                           "country",
                                                           "champions"});
    auto req = buildRequest({1, 3}, {"", ""}, {tag});
    auto* processor = ScanVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    // all 11 columns in value
    checkResponse(*resp.props_ref(), respTag, 11 + 1 /* kVid */, totalRowCount);
  }
}

TEST(ScanVertexTest, LimitTest) {
  fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  TagID player = 1;

  {
    LOG(INFO) << "Scan one tag with some properties in one batch";
    constexpr std::size_t limit = 3;
    size_t totalRowCount = 0;
    auto tag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    auto req = buildRequest({2}, {""}, {tag}, limit);
    auto* processor = ScanVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    checkResponse(*resp.props_ref(), tag, tag.second.size() + 1 /* kVid */, totalRowCount);
    EXPECT_EQ(totalRowCount, limit);
  }
  {
    LOG(INFO) << "Scan one tag with all properties in one batch";
    constexpr std::size_t limit = 3;
    size_t totalRowCount = 0;
    auto tag = std::make_pair(player, std::vector<std::string>{});
    auto respTag = std::make_pair(player,
                                  std::vector<std::string>{"name",
                                                           "age",
                                                           "playing",
                                                           "career",
                                                           "startYear",
                                                           "endYear",
                                                           "games",
                                                           "avgScore",
                                                           "serveTeams",
                                                           "country",
                                                           "champions"});
    auto req = buildRequest({2}, {""}, {tag}, limit);
    auto* processor = ScanVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    // all 11 columns in value
    checkResponse(*resp.props_ref(), respTag, 11 + 1 /* kVid */, totalRowCount);
    EXPECT_EQ(totalRowCount, limit);
  }
}

TEST(ScanVertexTest, MultipleTagsTest) {
  fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  TagID player = 1;
  TagID team = 2;

  {
    LOG(INFO) << "Scan one tag with some properties in one batch";
    //    size_t totalRowCount = 0;
    auto playerTag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    auto teamTag = std::make_pair(team, std::vector<std::string>{kTag, "name"});
    auto req = buildRequest({1}, {""}, {playerTag, teamTag});
    auto* processor = ScanVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    nebula::DataSet expect(
        {"_vid", "1._vid", "1._tag", "1.name", "1.age", "1.avgScore", "2._tag", "2.name"});
    expect.emplace_back(List({"Bulls",
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              2,
                              "Bulls"}));
    expect.emplace_back(List({"Cavaliers",
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              2,
                              "Cavaliers"}));
    expect.emplace_back(List({"Damian Lillard",
                              "Damian Lillard",
                              1,
                              "Damian Lillard",
                              29,
                              24,
                              Value::kEmpty,
                              Value::kEmpty}));
    expect.emplace_back(List(
        {"Jason Kidd", "Jason Kidd", 1, "Jason Kidd", 47, 12.6, Value::kEmpty, Value::kEmpty}));
    expect.emplace_back(List(
        {"Kevin Durant", "Kevin Durant", 1, "Kevin Durant", 31, 27, Value::kEmpty, Value::kEmpty}));
    expect.emplace_back(List(
        {"Kobe Bryant", "Kobe Bryant", 1, "Kobe Bryant", 41, 25, Value::kEmpty, Value::kEmpty}));
    expect.emplace_back(List({"Kristaps Porzingis",
                              "Kristaps Porzingis",
                              1,
                              "Kristaps Porzingis",
                              24,
                              18.1,
                              Value::kEmpty,
                              Value::kEmpty}));
    expect.emplace_back(List(
        {"Luka Doncic", "Luka Doncic", 1, "Luka Doncic", 21, 24.4, Value::kEmpty, Value::kEmpty}));
    expect.emplace_back(List({"Mavericks",
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              2,
                              "Mavericks"}));
    expect.emplace_back(List({"Nuggets",
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              Value::kEmpty,
                              2,
                              "Nuggets"}));
    expect.emplace_back(List(
        {"Paul George", "Paul George", 1, "Paul George", 30, 19.9, Value::kEmpty, Value::kEmpty}));
    expect.emplace_back(List({"Tracy McGrady",
                              "Tracy McGrady",
                              1,
                              "Tracy McGrady",
                              41,
                              19.6,
                              Value::kEmpty,
                              Value::kEmpty}));
    expect.emplace_back(List({"Vince Carter",
                              "Vince Carter",
                              1,
                              "Vince Carter",
                              43,
                              16.7,
                              Value::kEmpty,
                              Value::kEmpty}));
    EXPECT_EQ(expect, *resp.props_ref());
  }
}

TEST(ScanVertexTest, FilterTest) {
  fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

  TagID player = 1;
  TagID team = 2;
  ObjectPool pool;

  {
    LOG(INFO) << "Scan one tag with some properties in one batch";
    //    size_t totalRowCount = 0;
    auto playerTag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    auto teamTag = std::make_pair(team, std::vector<std::string>{kTag, "name"});
    auto req = buildRequest({1}, {""}, {playerTag, teamTag});
    Expression* filter = TagPropertyExpression::make(&pool, "1", "name");
    filter =
        RelationalExpression::makeEQ(&pool, filter, ConstantExpression::make(&pool, "Kobe Bryant"));
    req.filter_ref() = filter->encode();
    auto* processor = ScanVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    nebula::DataSet expect(
        {"_vid", "1._vid", "1._tag", "1.name", "1.age", "1.avgScore", "2._tag", "2.name"});
    expect.emplace_back(List(
        {"Kobe Bryant", "Kobe Bryant", 1, "Kobe Bryant", 41, 25, Value::kEmpty, Value::kEmpty}));
    EXPECT_EQ(expect, *resp.props_ref());
  }
  {
    LOG(INFO) << "Scan one tag with some properties in one batch";
    //    size_t totalRowCount = 0;
    auto playerTag =
        std::make_pair(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
    auto teamTag = std::make_pair(team, std::vector<std::string>{kTag, "name"});
    auto req = buildRequest({1}, {""}, {playerTag, teamTag});
    Expression* filter = TagPropertyExpression::make(&pool, "1", "name");
    filter =
        RelationalExpression::makeEQ(&pool, filter, ConstantExpression::make(&pool, "Kobe Bryant"));
    filter = LogicalExpression::makeAnd(
        &pool,
        filter,
        UnaryExpression::makeIsEmpty(&pool, TagPropertyExpression::make(&pool, "2", "name")));
    req.filter_ref() = filter->encode();
    auto* processor = ScanVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    nebula::DataSet expect(
        {"_vid", "1._vid", "1._tag", "1.name", "1.age", "1.avgScore", "2._tag", "2.name"});
    expect.emplace_back(List(
        {"Kobe Bryant", "Kobe Bryant", 1, "Kobe Bryant", 41, 25, Value::kEmpty, Value::kEmpty}));
    EXPECT_EQ(expect, *resp.props_ref());
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
