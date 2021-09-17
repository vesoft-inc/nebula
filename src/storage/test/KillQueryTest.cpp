/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>

#include "clients/meta/MetaClient.h"
#include "common/fs/TempDir.h"
#include "interface/gen-cpp2/common_types.h"
#include "mock/MockCluster.h"
#include "storage/index/LookupProcessor.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"

DECLARE_int32(check_plan_killed_frequency);
namespace nebula {
namespace meta {
class KillQueryMetaWrapper {
 public:
  explicit KillQueryMetaWrapper(MetaClient* client) : client_(client) {}
  void killQuery(SessionID session_id, ExecutionPlanID plan_id) {
    client_->killedPlans_.load()->emplace(session_id, plan_id);
  }

 private:
  MetaClient* client_;
};
}  // namespace meta
namespace storage {

class KillQueryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_check_plan_killed_frequency = 0;
    storagePath_ = new fs::TempDir("/tmp/KillQueryTest.storage.XXXXXX");
    cluster_ = new mock::MockCluster{};
    cluster_->initStorageKV(storagePath_->path());
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    metaClient_ = std::make_unique<meta::MetaClient>(threadPool,
                                                     std::vector<HostAddr>{HostAddr{"0.0.0.0", 0}});
    auto env = cluster_->storageEnv_.get();
    env->metaClient_ = metaClient_.get();
    client_ = std::make_unique<meta::KillQueryMetaWrapper>(metaClient_.get());
  }
  void TearDown() override {
    delete storagePath_;
    delete cluster_;
  }

 protected:
  fs::TempDir* storagePath_;
  mock::MockCluster* cluster_;
  std::unique_ptr<meta::MetaClient> metaClient_;
  std::unique_ptr<meta::KillQueryMetaWrapper> client_;
};

TEST_F(KillQueryTest, GetNeighbors) {
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  client_->killQuery(1, 1);
  auto totalParts = cluster_->getTotalParts();
  auto env = cluster_->storageEnv_.get();
  auto processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
  ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
  TagID player = 1;
  EdgeType serve = 101;
  std::vector<VertexID> vertices = {"Tim Duncan"};
  std::vector<EdgeType> over = {serve};
  std::vector<std::pair<TagID, std::vector<std::string>>> tags;
  std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
  tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
  edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});

  auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
  cpp2::RequestCommon common;
  common.set_session_id(1);
  common.set_plan_id(1);
  req.set_common(common);
  auto fut = processor->getFuture();
  processor->process(req);
  cpp2::GetNeighborsResponse resp = std::move(fut).get();
  auto part_count = req.get_parts().size();
  auto failed_part_count = resp.get_result().get_failed_parts().size();
  ASSERT_EQ(part_count, failed_part_count);
  ASSERT_NE(part_count, 0);
  for (auto& part : resp.get_result().get_failed_parts()) {
    ASSERT_EQ(part.get_code(), ::nebula::cpp2::ErrorCode::E_PLAN_IS_KILLED);
  }
}
TEST_F(KillQueryTest, TagIndex) {
  auto env = cluster_->storageEnv_.get();
  GraphSpaceID spaceId = 1;
  auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
  ASSERT_TRUE(vIdLen.ok());
  auto totalParts = cluster_->getTotalParts();
  ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
  {
    client_->killQuery(1, 1);
    auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
    cpp2::LookupIndexRequest req;
    nebula::storage::cpp2::IndexSpec indices;
    req.set_space_id(spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.set_tag_id(1);
    indices.set_schema_id(schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.set_parts(std::move(parts));
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kTag);
    returnCols.emplace_back("age");
    req.set_return_columns(std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    std::string name = "Rudy Gay";
    columnHint.set_begin_value(Value(name));
    columnHint.set_column_name("name");
    columnHint.set_scan_type(cpp2::ScanType::PREFIX);
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.set_column_hints(std::move(columnHints));
    context1.set_filter("");
    context1.set_index_id(1);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.set_contexts(std::move(contexts));
    req.set_indices(std::move(indices));
    cpp2::RequestCommon common;
    common.set_session_id(1);
    common.set_plan_id(1);
    req.set_common(common);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    ASSERT_EQ(resp.get_data()->size(), 0);
    auto part_count = req.get_parts().size();
    auto failed_part_count = resp.get_result().get_failed_parts().size();
    ASSERT_EQ(part_count, failed_part_count);
    ASSERT_NE(part_count, 0);
    for (auto& part : resp.get_result().get_failed_parts()) {
      ASSERT_EQ(part.get_code(), ::nebula::cpp2::ErrorCode::E_PLAN_IS_KILLED);
    }
  }
}
TEST_F(KillQueryTest, EdgeIndex) {
  auto env = cluster_->storageEnv_.get();
  GraphSpaceID spaceId = 1;
  auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
  ASSERT_TRUE(vIdLen.ok());
  auto totalParts = cluster_->getTotalParts();
  ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
  ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, true));
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
  {
    client_->killQuery(1, 1);
    auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
    cpp2::LookupIndexRequest req;
    nebula::storage::cpp2::IndexSpec indices;
    req.set_space_id(spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.set_edge_type(102);
    indices.set_schema_id(schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.set_parts(std::move(parts));
    std::string tony = "Tony Parker";
    std::string manu = "Manu Ginobili";
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kSrc);
    returnCols.emplace_back(kType);
    returnCols.emplace_back(kRank);
    returnCols.emplace_back(kDst);
    returnCols.emplace_back("teamName");
    req.set_return_columns(std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    columnHint.set_begin_value(Value(tony));
    columnHint.set_column_name("player1");
    columnHint.set_scan_type(cpp2::ScanType::PREFIX);
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.set_column_hints(std::move(columnHints));
    context1.set_filter("");
    context1.set_index_id(102);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.set_contexts(std::move(contexts));
    req.set_indices(std::move(indices));
    cpp2::RequestCommon common;
    common.set_session_id(1);
    common.set_plan_id(1);
    req.set_common(common);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    ASSERT_EQ(resp.get_data()->size(), 0);
    auto part_count = req.get_parts().size();
    auto failed_part_count = resp.get_result().get_failed_parts().size();
    ASSERT_EQ(part_count, failed_part_count);
    ASSERT_NE(part_count, 0);
    for (auto& part : resp.get_result().get_failed_parts()) {
      ASSERT_EQ(part.get_code(), ::nebula::cpp2::ErrorCode::E_PLAN_IS_KILLED);
    }
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
