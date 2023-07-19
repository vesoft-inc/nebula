/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/NebulaKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/index/LookupProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"

namespace nebula {
namespace storage {

std::string convertVertexId(size_t vIdLen, int32_t vId) {
  std::string id;
  id.reserve(vIdLen);
  id.append(reinterpret_cast<const char*>(&vId), sizeof(vId)).append(vIdLen - sizeof(vId), '\0');
  return id;
}

int64_t verifyResultNum(GraphSpaceID spaceId,
                        PartitionID partId,
                        const std::string& prefix,
                        nebula::kvstore::KVStore* kv) {
  std::unique_ptr<kvstore::KVIterator> iter;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, kv->prefix(spaceId, partId, prefix, &iter));
  int64_t rowCount = 0;
  while (iter->valid()) {
    rowCount++;
    iter->next();
  }
  return rowCount;
}

TEST(IndexTest, SimpleVerticesTest) {
  fs::TempDir rootPath("/tmp/SimpleVerticesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  // verify insert
  {
    cpp2::AddVerticesRequest req;
    req.space_id_ref() = 1;
    req.if_not_exists_ref() = true;
    // mock v2 vertices
    for (auto partId = 1; partId <= 6; partId++) {
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = 3;
      const Date date = {2020, 2, 20};
      const DateTime dt = {2020, 2, 20, 10, 30, 45, 0};
      std::vector<Value> props;
      props.emplace_back(Value(true));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1.1f));
      props.emplace_back(Value(1.1f));
      props.emplace_back(Value("string"));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(std::move(date)));
      props.emplace_back(Value(std::move(dt)));
      newTag.props_ref() = std::move(props);
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = convertVertexId(vIdLen, partId);
      newVertex.tags_ref() = std::move(newTags);
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check insert data...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = NebulaKeyUtils::tagPrefix(vIdLen, partId, convertVertexId(vIdLen, partId));
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }

    LOG(INFO) << "Check insert index...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, 3);
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }
  }
  // verify delete
  {
    auto* processor = DeleteVerticesProcessor::instance(env, nullptr);
    cpp2::DeleteVerticesRequest req;
    req.space_id_ref() = 1;
    for (auto partId = 1; partId <= 6; partId++) {
      std::vector<Value> vertices;
      vertices.emplace_back(Value(convertVertexId(vIdLen, partId)));
      (*req.parts_ref())[partId] = std::move(vertices);
    }
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check delete data...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = NebulaKeyUtils::tagPrefix(vIdLen, partId, convertVertexId(vIdLen, partId));
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(0, retNum);
    }

    LOG(INFO) << "Check delete index...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, 3);
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(0, retNum);
    }
  }
}

TEST(IndexTest, SimpleEdgesTest) {
  fs::TempDir rootPath("/tmp/SimpleEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  // verify insert
  {
    cpp2::AddEdgesRequest req;
    req.space_id_ref() = 1;
    req.if_not_exists_ref() = true;
    // mock v2 edges
    for (auto partId = 1; partId <= 6; partId++) {
      nebula::storage::cpp2::NewEdge newEdge;
      nebula::storage::cpp2::EdgeKey edgeKey;
      edgeKey.src_ref() = convertVertexId(vIdLen, partId);
      edgeKey.edge_type_ref() = 101;
      edgeKey.ranking_ref() = 0;
      edgeKey.dst_ref() = convertVertexId(vIdLen, partId + 6);
      newEdge.key_ref() = std::move(edgeKey);
      std::vector<Value> props;
      props.emplace_back(Value("col1"));
      props.emplace_back(Value("col2"));
      props.emplace_back(Value(3L));
      props.emplace_back(Value(4L));
      props.emplace_back(Value(5L));
      props.emplace_back(Value(6L));
      props.emplace_back(Value(7.7F));
      newEdge.props_ref() = std::move(props);
      (*req.parts_ref())[partId].emplace_back(newEdge);
      (*newEdge.key_ref()).edge_type_ref() = -101;
      (*req.parts_ref())[partId].emplace_back(std::move(newEdge));
    }
    auto* processor = AddEdgesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check insert data...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = NebulaKeyUtils::edgePrefix(partId);
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(2, retNum);
    }

    LOG(INFO) << "Check insert index...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, 101);
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }
  }
  // verify delete
  {
    auto* processor = DeleteEdgesProcessor::instance(env, nullptr);
    cpp2::DeleteEdgesRequest req;
    req.space_id_ref() = 1;
    for (auto partId = 1; partId <= 6; partId++) {
      nebula::storage::cpp2::EdgeKey edgeKey;
      edgeKey.src_ref() = convertVertexId(vIdLen, partId);
      edgeKey.edge_type_ref() = 101;
      edgeKey.ranking_ref() = 0;
      edgeKey.dst_ref() = convertVertexId(vIdLen, partId + 6);
      (*req.parts_ref())[partId].emplace_back(edgeKey);
      edgeKey.edge_type_ref() = -101;
      (*req.parts_ref())[partId].emplace_back(std::move(edgeKey));
    }
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check delete data...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = NebulaKeyUtils::edgePrefix(partId);
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(0, retNum);
    }

    LOG(INFO) << "Check delete index...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, 101);
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(0, retNum);
    }
  }
}

/**
 * Test nullable and default value for vertex insert.
 * And verify the correctness of the nullable and default value.
 **/
TEST(IndexTest, VerticesValueTest) {
  GraphSpaceID spaceId = 1;
  TagID tagId = 111;
  IndexID indexId = 111;
  fs::TempDir rootPath("/tmp/VerticesValueTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
  auto pool = &cluster.pool_;

  // Mock a tag schema for nullable column and default column.
  {
    auto* schemaMan = reinterpret_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
    schemaMan->addTagSchema(spaceId, tagId, mock::MockData::mockTypicaSchemaV2(pool));
  }
  // Mock an index for nullable column and default column.
  {
    auto* indexMan = reinterpret_cast<mock::AdHocIndexManager*>(env->indexMan_);
    indexMan->addTagIndex(spaceId, tagId, indexId, mock::MockData::mockTypicaIndexColumns());
  }
  // verify insert
  {
    cpp2::AddVerticesRequest req;
    req.space_id_ref() = spaceId;
    req.if_not_exists_ref() = true;
    std::unordered_map<int, std::vector<std::string>> propNames;
    propNames[tagId] = {
        "col_bool", "col_int", "col_float", "col_float_null", "col_str", "col_date"};
    req.prop_names_ref() = std::move(propNames);
    // mock v2 vertices
    for (auto partId = 1; partId <= 6; partId++) {
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = tagId;
      const Date date = {2020, 1, 20};
      std::vector<Value> props;
      props.emplace_back(Value(true));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1.1f));
      props.emplace_back(Value(5.5f));
      props.emplace_back(Value("string"));
      props.emplace_back(Value(std::move(date)));
      newTag.props_ref() = std::move(props);
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = convertVertexId(vIdLen, partId);
      newVertex.tags_ref() = std::move(newTags);
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check insert data...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = NebulaKeyUtils::tagPrefix(vIdLen, partId, convertVertexId(vIdLen, partId));
      auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }

    LOG(INFO) << "Check insert index...";
    std::vector<Value> values;
    std::vector<Value::Type> colsType;
    Value nullValue = Value(NullType::__NULL__);
    // col_bool
    values.emplace_back(Value(true));
    // col_bool_null
    values.emplace_back(nullValue);
    // col_bool_default
    values.emplace_back(Value(true));
    // col_int
    values.emplace_back(Value(1L));
    // col_int_null
    values.emplace_back(nullValue);
    // col_float
    values.emplace_back(Value(1.1f));
    // col_float_null
    values.emplace_back(Value(5.5f));
    // col_str
    values.emplace_back(IndexKeyUtils::encodeValue(Value("string"), 20));
    // col_str_null
    values.emplace_back(nullValue);
    // col_date
    const Date date = {2020, 1, 20};
    values.emplace_back(Value(date));
    // col_date_null
    values.emplace_back(nullValue);
    auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
    indexItem->fields_ref() = mock::MockData::mockTypicaIndexColumns();
    auto indexes = IndexKeyUtils::encodeValues(std::move(values), indexItem.get());

    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, indexId);
      auto indexKey = IndexKeyUtils::vertexIndexKeys(
          vIdLen, partId, indexId, convertVertexId(vIdLen, partId), std::move(indexes))[0];
      std::unique_ptr<kvstore::KVIterator> iter;
      auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
      int64_t rowCount = 0;
      while (iter->valid()) {
        EXPECT_EQ(indexKey, iter->key().str());
        rowCount++;
        iter->next();
      }
      EXPECT_EQ(1, rowCount);
    }
  }
}

/**
 * Alter tag test.
 * If a tag is attached to an index. allows to add new columns.
 * Test the old index work well.
 * And then create new index with newly added columns.
 * Test the all indexes works well.
 **/
TEST(IndexTest, AlterTagIndexTest) {
  GraphSpaceID spaceId = 1;
  TagID tagId = 111;
  IndexID indexId1 = 222;
  IndexID indexId2 = 333;
  fs::TempDir rootPath("/tmp/AlterTagIndexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();

  // Mock a tag schema for nullable column and default column.
  {
    auto* schemaMan = reinterpret_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
    schemaMan->addTagSchema(spaceId, tagId, mock::MockData::mockGeneralTagSchemaV1());
  }
  // Mock an index for nullable column and default column.
  {
    auto* indexMan = reinterpret_cast<mock::AdHocIndexManager*>(env->indexMan_);
    indexMan->addTagIndex(spaceId, tagId, indexId1, mock::MockData::mockGeneralTagIndexColumns());
  }
  // verify insert
  {
    cpp2::AddVerticesRequest req;
    req.space_id_ref() = spaceId;
    req.if_not_exists_ref() = true;
    // mock v2 vertices
    for (auto partId = 1; partId <= 6; partId++) {
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = tagId;
      std::vector<Value> props;
      props.emplace_back(Value(true));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1.1f));
      props.emplace_back(Value(1.1f));
      props.emplace_back(Value("string"));
      newTag.props_ref() = std::move(props);
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = convertVertexId(vIdLen, partId);
      newVertex.tags_ref() = std::move(newTags);
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check insert data...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = NebulaKeyUtils::tagPrefix(vIdLen, partId, convertVertexId(vIdLen, partId));
      auto retNum = verifyResultNum(spaceId, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }

    LOG(INFO) << "Check insert index...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, indexId1);
      auto retNum = verifyResultNum(spaceId, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }
  }

  // Alter tag add new columns.
  {
    auto* schemaMan = reinterpret_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
    schemaMan->addTagSchema(spaceId, tagId, mock::MockData::mockGeneralTagSchemaV2());
  }
  // create new index with newly added columns.
  {
    auto* indexMan = reinterpret_cast<mock::AdHocIndexManager*>(env->indexMan_);
    indexMan->addTagIndex(spaceId, tagId, indexId2, mock::MockData::mockSimpleTagIndexColumns());
  }
  // verify insert
  {
    cpp2::AddVerticesRequest req;
    req.space_id_ref() = spaceId;
    req.if_not_exists_ref() = false;
    // mock v2 vertices
    for (auto partId = 1; partId <= 6; partId++) {
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = tagId;
      const Date date = {2020, 2, 20};
      const DateTime dt = {2020, 2, 20, 10, 30, 45, 0};
      std::vector<Value> props;
      props.emplace_back(Value(true));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1.1f));
      props.emplace_back(Value(1.1f));
      props.emplace_back(IndexKeyUtils::encodeValue(Value("string"), 20));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(1L));
      props.emplace_back(Value(std::move(date)));
      props.emplace_back(Value(std::move(dt)));
      newTag.props_ref() = std::move(props);
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = convertVertexId(vIdLen, partId);
      newVertex.tags_ref() = std::move(newTags);
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check insert data...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = NebulaKeyUtils::tagPrefix(vIdLen, partId, convertVertexId(vIdLen, partId));
      auto retNum = verifyResultNum(spaceId, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }

    LOG(INFO) << "Check insert index1...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, indexId1);
      auto retNum = verifyResultNum(spaceId, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
    }

    LOG(INFO) << "Check insert index2...";
    for (auto partId = 1; partId <= 6; partId++) {
      auto prefix = IndexKeyUtils::indexPrefix(partId, indexId2);
      auto retNum = verifyResultNum(spaceId, partId, prefix, env->kvstore_);
      EXPECT_EQ(1, retNum);
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
