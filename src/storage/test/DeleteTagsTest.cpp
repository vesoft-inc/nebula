
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/NebulaKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/index/LookupProcessor.h"
#include "storage/mutate/DeleteTagsProcessor.h"
#include "storage/query/GetPropProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

cpp2::GetPropRequest buildGetPropRequest(
    int32_t totalParts,
    const std::vector<VertexID>& vertices,
    const std::vector<std::pair<TagID, std::vector<std::string>>>& tags) {
  std::hash<std::string> hash;
  cpp2::GetPropRequest req;
  req.set_space_id(1);
  for (const auto& vertex : vertices) {
    PartitionID partId = (hash(vertex) % totalParts) + 1;
    nebula::Row row;
    row.values.emplace_back(vertex);
    (*req.parts_ref())[partId].emplace_back(std::move(row));
  }

  std::vector<cpp2::VertexProp> vertexProps;
  if (tags.empty()) {
    req.set_vertex_props(std::move(vertexProps));
  } else {
    for (const auto& tag : tags) {
      TagID tagId = tag.first;
      cpp2::VertexProp tagProp;
      tagProp.set_tag(tagId);
      for (const auto& prop : tag.second) {
        (*tagProp.props_ref()).emplace_back(std::move(prop));
      }
      vertexProps.emplace_back(std::move(tagProp));
    }
    req.set_vertex_props(std::move(vertexProps));
  }
  return req;
}

cpp2::LookupIndexRequest buildLookupRequest(int32_t totalParts, std::string playerName) {
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.set_space_id(1);
  nebula::cpp2::SchemaID schemaId;
  schemaId.set_tag_id(1);
  indices.set_schema_id(schemaId);
  std::vector<PartitionID> parts;
  for (PartitionID partId = 1; partId <= totalParts; partId++) {
    parts.emplace_back(partId);
  }
  req.set_parts(std::move(parts));
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  returnCols.emplace_back("age");
  req.set_return_columns(std::move(returnCols));
  cpp2::IndexColumnHint columnHint;
  columnHint.set_begin_value(Value(playerName));
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
  return req;
}

cpp2::DeleteTagsRequest buildDeleteTagsRequest(int32_t totalParts,
                                               const std::vector<cpp2::DelTags>& delTags) {
  std::hash<std::string> hash;
  cpp2::DeleteTagsRequest req;
  req.set_space_id(1);
  for (const auto& delTag : delTags) {
    PartitionID partId = (hash(delTag.get_id().getStr()) % totalParts) + 1;
    (*req.parts_ref())[partId].emplace_back(std::move(delTag));
  }
  return req;
}

TEST(DeleteTagsTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/LookupIndexTestV1.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

  GraphSpaceID spaceId = 1;
  auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
  ASSERT_TRUE(vIdLen.ok());
  auto totalParts = cluster.getTotalParts();
  ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
  TagID player = 1;

  // before delete
  {
    std::vector<VertexID> vertices = {"Tim Duncan"};
    std::vector<std::pair<TagID, std::vector<std::string>>> tags;
    tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
    auto req = buildGetPropRequest(totalParts, vertices, tags);
    auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();

    ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
    ASSERT_EQ(1, resp.props_ref()->size());
  }
  {
    auto req = buildLookupRequest(totalParts, "Tim Duncan");
    auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();

    ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
    ASSERT_EQ(1, resp.data_ref()->size());
  }
  // do delete
  {
    std::vector<cpp2::DelTags> delTags;
    cpp2::DelTags delTag;
    delTag.set_id("Tim Duncan");
    (*delTag.tags_ref()).emplace_back(player);
    delTags.emplace_back(std::move(delTag));
    auto req = buildDeleteTagsRequest(totalParts, delTags);
    auto* processor = DeleteTagsProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();

    ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
  }
  // after delete
  {
    std::vector<VertexID> vertices = {"Tim Duncan"};
    std::vector<std::pair<TagID, std::vector<std::string>>> tags;
    tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
    auto req = buildGetPropRequest(totalParts, vertices, tags);
    auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();

    ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
    ASSERT_EQ(0, resp.props_ref()->size());
  }
  {
    auto req = buildLookupRequest(totalParts, "Tim Duncan");
    auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();

    ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
    ASSERT_EQ(0, resp.data_ref()->size());
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
