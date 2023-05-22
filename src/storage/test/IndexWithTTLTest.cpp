/* Copyright (c) 2021 vesoft inc. All rights reserved.
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
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "storage/index/LookupProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"

DECLARE_int32(min_level_for_custom_filter);

namespace nebula {
namespace storage {

ObjectPool objPool;
auto pool = &objPool;
int gJobId = 0;

std::string convertVertexId(size_t vIdLen, int32_t vId) {
  std::string id;
  id.reserve(vIdLen);
  id.append(reinterpret_cast<const char*>(&vId), sizeof(vId)).append(vIdLen - sizeof(vId), '\0');
  return id;
}

int64_t verifyResultNum(GraphSpaceID spaceId,
                        PartitionID partId,
                        const std::string& prefix,
                        nebula::kvstore::KVStore* kv,
                        int64_t ts = 0) {
  std::unique_ptr<kvstore::KVIterator> iter;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, kv->prefix(spaceId, partId, prefix, &iter));
  int64_t rowCount = 0;
  auto now = std::time(NULL);
  while (iter->valid()) {
    if (ts > 0) {
      EXPECT_TRUE(!iter->val().empty());
      auto ttl = IndexKeyUtils::parseIndexTTL(iter->val());
      EXPECT_TRUE(ttl.isInt());
      EXPECT_TRUE(ts <= ttl.getInt() && ttl.getInt() <= now);
    }
    rowCount++;
    iter->next();
  }
  return rowCount;
}

void createSchema(meta::SchemaManager* schemaMan,
                  int32_t schemaId,
                  int64_t duration,
                  bool isEdge = false) {
  auto* sm = reinterpret_cast<mock::AdHocSchemaManager*>(schemaMan);
  std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
  schema->addField("c1", nebula::cpp2::PropertyType::INT64, 0, false);
  schema->addField("c2",
                   nebula::cpp2::PropertyType::INT64,
                   0,
                   false,
                   ConstantExpression::make(pool, 0L)->encode());
  meta::cpp2::SchemaProp prop;
  prop.ttl_col_ref() = "c2";
  prop.ttl_duration_ref() = duration;
  schema->setProp(prop);
  if (isEdge) {
    sm->addEdgeSchema(1, schemaId, std::move(schema));
  } else {
    sm->addTagSchema(1, schemaId, std::move(schema));
  }
}

void createIndex(meta::IndexManager* indexMan,
                 int32_t schemaId,
                 IndexID indexId,
                 bool isEdge = false) {
  auto* im = reinterpret_cast<mock::AdHocIndexManager*>(indexMan);

  std::vector<nebula::meta::cpp2::ColumnDef> cols;
  meta::cpp2::ColumnDef col;
  col.name = "c1";
  col.type.type_ref() = nebula::cpp2::PropertyType::INT64;
  cols.emplace_back(std::move(col));
  if (isEdge) {
    im->addEdgeIndex(1, schemaId, indexId, std::move(cols));
  } else {
    im->addTagIndex(1, schemaId, indexId, std::move(cols));
  }
}

void insertVertex(storage::StorageEnv* env, size_t vIdLen, TagID tagId) {
  cpp2::AddVerticesRequest req;
  req.space_id_ref() = 1;
  req.if_not_exists_ref() = true;
  for (auto partId = 1; partId <= 6; partId++) {
    nebula::storage::cpp2::NewVertex newVertex;
    nebula::storage::cpp2::NewTag newTag;
    newTag.tag_id_ref() = tagId;
    std::vector<Value> props;
    props.emplace_back(Value(1L));
    props.emplace_back(Value(std::time(NULL)));
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
}

void insertEdge(storage::StorageEnv* env, size_t vIdLen, EdgeType edgeType) {
  cpp2::AddEdgesRequest req;
  req.space_id_ref() = 1;
  req.if_not_exists_ref() = true;
  for (auto partId = 1; partId <= 6; partId++) {
    nebula::storage::cpp2::NewEdge newEdge;
    nebula::storage::cpp2::EdgeKey edgeKey;
    edgeKey.src_ref() = convertVertexId(vIdLen, partId);
    edgeKey.edge_type_ref() = edgeType;
    edgeKey.ranking_ref() = 0;
    edgeKey.dst_ref() = convertVertexId(vIdLen, partId + 6);
    newEdge.key_ref() = std::move(edgeKey);
    std::vector<Value> props;
    props.emplace_back(Value(1L));
    props.emplace_back(Value(std::time(NULL)));
    newEdge.props_ref() = std::move(props);
    (*req.parts_ref())[partId].emplace_back(newEdge);
    (*newEdge.key_ref()).edge_type_ref() = -edgeType;
    (*req.parts_ref())[partId].emplace_back(std::move(newEdge));
  }
  auto* processor = AddEdgesProcessor::instance(env, nullptr);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());
}

TEST(IndexWithTTLTest, AddVerticesIndexWithTTL) {
  fs::TempDir rootPath("/tmp/AddVerticesIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, true, false, {}, true);
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 1, false);
  createIndex(env->indexMan_, 2021001, 2021002, false);
  insertVertex(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(1, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(1, retNum);
  }

  sleep(2);

  LOG(INFO) << "Do compaction";
  FLAGS_min_level_for_custom_filter = -1;
  auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
  ns->compact(1);

  LOG(INFO) << "Check data after compaction ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(0, retNum);
  }

  LOG(INFO) << "Check index after compaction ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }
}

TEST(IndexWithTTLTest, AddEdgesIndexWithTTL) {
  fs::TempDir rootPath("/tmp/AddEdgesIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, true, false, {}, true);
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 1, true);
  createIndex(env->indexMan_, 2021001, 2021002, true);
  insertEdge(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(2, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(1, retNum);
  }

  sleep(2);

  LOG(INFO) << "Do compaction";
  FLAGS_min_level_for_custom_filter = -1;
  auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
  ns->compact(1);

  LOG(INFO) << "Check data after compaction ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(0, retNum);
  }

  LOG(INFO) << "Check index after compaction ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }
}

TEST(IndexWithTTLTest, UpdateVerticesIndexWithTTL) {
  fs::TempDir rootPath("/tmp/UpdateVerticesIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 1800, false);
  createIndex(env->indexMan_, 2021001, 2021002, false);
  insertVertex(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(1, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(1, retNum);
  }

  for (auto partId = 1; partId <= 6; partId++) {
    cpp2::UpdateVertexRequest req;
    req.space_id_ref() = 1;
    VertexID vertexId = convertVertexId(vIdLen, partId);
    req.part_id_ref() = partId;
    req.vertex_id_ref() = vertexId;
    req.tag_id_ref() = 2021001;

    std::vector<cpp2::UpdatedProp> updatedProps;
    cpp2::UpdatedProp uProp1;
    uProp1.name_ref() = "c1";
    const auto& val1 = *ConstantExpression::make(pool, 2L);
    uProp1.value_ref() = Expression::encode(val1);
    updatedProps.emplace_back(uProp1);

    cpp2::UpdatedProp uProp2;
    uProp2.name_ref() = "c2";
    const auto& val2 = *ConstantExpression::make(pool, 5555L);
    uProp2.value_ref() = Expression::encode(val2);
    updatedProps.emplace_back(uProp2);
    req.updated_props_ref() = std::move(updatedProps);
    req.insertable_ref() = false;

    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

  LOG(INFO) << "Check data after update ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(1, retNum);
  }

  LOG(INFO) << "Check index after update ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, 5555);
    EXPECT_EQ(1, retNum);
  }
}

TEST(IndexWithTTLTest, UpdateEdgesIndexWithTTL) {
  fs::TempDir rootPath("/tmp/UpdateEdgesIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 1800, true);
  createIndex(env->indexMan_, 2021001, 2021002, true);
  insertEdge(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(2, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(1, retNum);
  }

  for (auto partId = 1; partId <= 6; partId++) {
    cpp2::UpdateEdgeRequest req;
    req.space_id_ref() = 1;
    req.part_id_ref() = partId;
    VertexID srcId = convertVertexId(vIdLen, partId);
    VertexID dstId = convertVertexId(vIdLen, partId + 6);
    EdgeRanking rank = 0;
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.src_ref() = srcId;
    edgeKey.edge_type_ref() = 2021001;
    edgeKey.ranking_ref() = rank;
    edgeKey.dst_ref() = dstId;
    req.edge_key_ref() = edgeKey;

    std::vector<cpp2::UpdatedProp> updatedProps;
    cpp2::UpdatedProp uProp1;
    uProp1.name_ref() = "c1";
    const auto& val1 = *ConstantExpression::make(pool, 2L);
    uProp1.value_ref() = Expression::encode(val1);
    updatedProps.emplace_back(uProp1);

    cpp2::UpdatedProp uProp2;
    uProp2.name_ref() = "c2";
    const auto& val2 = *ConstantExpression::make(pool, 5555L);
    uProp2.value_ref() = Expression::encode(val2);
    updatedProps.emplace_back(uProp2);
    req.updated_props_ref() = std::move(updatedProps);
    req.insertable_ref() = false;

    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

  LOG(INFO) << "Check data after update ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(2, retNum);
  }

  LOG(INFO) << "Check index after update ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, 5555);
    EXPECT_EQ(1, retNum);
  }
}

TEST(IndexWithTTLTest, RebuildTagIndexWithTTL) {
  fs::TempDir rootPath("/tmp/RebuildTagIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 180000, false);
  insertVertex(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(1, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }

  createIndex(env->indexMan_, 2021001, 2021002, false);

  auto manager_ = AdminTaskManager::instance();
  manager_->init();
  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {"2021002"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 13;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<RebuildTagIndexTask>(env, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    sleep(1);
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  manager_->shutdown();

  LOG(INFO) << "Check data after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(1, retNum);
  }

  LOG(INFO) << "Check index after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(1, retNum);
  }
}

TEST(IndexWithTTLTest, RebuildEdgeIndexWithTTL) {
  fs::TempDir rootPath("/tmp/RebuildEdgeIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 180000, true);
  insertEdge(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(2, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }

  createIndex(env->indexMan_, 2021001, 2021002, true);

  auto manager_ = AdminTaskManager::instance();
  manager_->init();
  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {"2021002"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_EDGE_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 13;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    sleep(1);
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  manager_->shutdown();

  LOG(INFO) << "Check data after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(2, retNum);
  }

  LOG(INFO) << "Check index after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(1, retNum);
  }
}

TEST(IndexWithTTLTest, RebuildTagIndexWithTTLExpired) {
  fs::TempDir rootPath("/tmp/RebuildTagIndexWithTTLExpired.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 1, false);
  insertVertex(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(1, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }

  createIndex(env->indexMan_, 2021001, 2021002, false);

  sleep(2);

  auto manager_ = AdminTaskManager::instance();
  manager_->init();
  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {"2021002"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 13;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<RebuildTagIndexTask>(env, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    sleep(1);
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  manager_->shutdown();

  LOG(INFO) << "Check data after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::tagPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(1, retNum);
  }

  LOG(INFO) << "Check index after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }
}

TEST(IndexWithTTLTest, RebuildEdgeIndexWithTTLExpired) {
  fs::TempDir rootPath("/tmp/RebuildEdgeIndexWithTTLExpired.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  auto beginTime = std::time(NULL);
  createSchema(env->schemaMan_, 2021001, 1, true);
  insertEdge(env, vIdLen, 2021001);

  LOG(INFO) << "Check insert data...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(2, retNum);
  }

  LOG(INFO) << "Check insert index...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }

  createIndex(env->indexMan_, 2021001, 2021002, true);

  sleep(2);

  auto manager_ = AdminTaskManager::instance();
  manager_->init();
  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {"2021002"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_EDGE_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 15;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    sleep(1);
  } while (!manager_->isFinished(context.jobId_, context.taskId_));
  manager_->shutdown();

  LOG(INFO) << "Check data after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
    EXPECT_EQ(2, retNum);
  }

  LOG(INFO) << "Check index after rebuild ...";
  for (auto partId = 1; partId <= 6; partId++) {
    auto prefix = IndexKeyUtils::indexPrefix(partId);
    auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_, beginTime);
    EXPECT_EQ(0, retNum);
  }
}

TEST(IndexWithTTLTest, LookupTagIndexWithTTL) {
  fs::TempDir rootPath("/tmp/LookupTagIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, true, false, {}, true);
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  createSchema(env->schemaMan_, 2021001, 180000, false);
  createIndex(env->indexMan_, 2021001, 2021002, false);
  insertVertex(env, vIdLen, 2021001);

  auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = 1;
  nebula::cpp2::SchemaID schemaId;
  schemaId.tag_id_ref() = 2021001;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= 6; p++) {
    parts.emplace_back(p);
  }
  req.parts_ref() = std::move(parts);
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  req.return_columns_ref() = std::move(returnCols);
  auto expr = RelationalExpression::makeNE(pool,
                                           TagPropertyExpression::make(pool, "2021001", "c1"),
                                           ConstantExpression::make(pool, Value(34L)));
  cpp2::IndexQueryContext context1;
  context1.filter_ref() = expr->encode();
  context1.index_id_ref() = 2021002;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());
  EXPECT_EQ(6, resp.get_data()->rows.size());
}

TEST(IndexWithTTLTest, LookupEdgeIndexWithTTL) {
  fs::TempDir rootPath("/tmp/LookupEdgeIndexWithTTL.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, true, false, {}, true);
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  createSchema(env->schemaMan_, 2021001, 180000, true);
  createIndex(env->indexMan_, 2021001, 2021002, true);
  insertEdge(env, vIdLen, 2021001);

  auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = 1;
  nebula::cpp2::SchemaID schemaId;
  schemaId.edge_type_ref() = 2021001;
  indices.schema_id_ref() = schemaId;

  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= 6; p++) {
    parts.emplace_back(p);
  }
  req.parts_ref() = std::move(parts);
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  req.return_columns_ref() = std::move(returnCols);
  auto expr = RelationalExpression::makeNE(pool,
                                           TagPropertyExpression::make(pool, "2021001", "c1"),
                                           ConstantExpression::make(pool, Value(34L)));
  cpp2::IndexQueryContext context1;
  context1.filter_ref() = expr->encode();
  context1.index_id_ref() = 2021002;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());
  EXPECT_EQ(6, resp.get_data()->rows.size());
}

TEST(IndexWithTTLTest, LookupTagIndexWithTTLExpired) {
  fs::TempDir rootPath("/tmp/LookupTagIndexWithTTLExpired.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, true, false, {}, true);
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  createSchema(env->schemaMan_, 2021001, 1, false);
  createIndex(env->indexMan_, 2021001, 2021002, false);
  insertVertex(env, vIdLen, 2021001);

  sleep(2);

  auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = 1;
  nebula::cpp2::SchemaID schemaId;
  schemaId.tag_id_ref() = 2021001;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= 6; p++) {
    parts.emplace_back(p);
  }
  req.parts_ref() = std::move(parts);
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  req.return_columns_ref() = std::move(returnCols);
  auto expr = RelationalExpression::makeNE(pool,
                                           TagPropertyExpression::make(pool, "2021001", "c1"),
                                           ConstantExpression::make(pool, Value(34L)));
  cpp2::IndexQueryContext context1;
  context1.filter_ref() = expr->encode();
  context1.index_id_ref() = 2021002;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());
  EXPECT_EQ(0, resp.get_data()->rows.size());
}

TEST(IndexWithTTLTest, LookupEdgeIndexWithTTLExpired) {
  fs::TempDir rootPath("/tmp/LookupEdgeIndexWithTTLExpired.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, true, false, {}, true);
  auto* env = cluster.storageEnv_.get();
  auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

  createSchema(env->schemaMan_, 2021001, 1, true);
  createIndex(env->indexMan_, 2021001, 2021002, true);
  insertEdge(env, vIdLen, 2021001);

  sleep(2);

  auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = 1;
  nebula::cpp2::SchemaID schemaId;
  schemaId.edge_type_ref() = 2021001;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= 6; p++) {
    parts.emplace_back(p);
  }
  req.parts_ref() = std::move(parts);
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  req.return_columns_ref() = std::move(returnCols);
  auto expr = RelationalExpression::makeNE(pool,
                                           TagPropertyExpression::make(pool, "2021001", "c1"),
                                           ConstantExpression::make(pool, Value(34L)));
  cpp2::IndexQueryContext context1;
  context1.filter_ref() = expr->encode();
  context1.index_id_ref() = 2021002;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());
  EXPECT_EQ(0, resp.get_data()->rows.size());
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
