/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include <filesystem>

#include "codec/RowWriterV2.h"
#include "common/base/Base.h"
#include "common/datatypes/Vector.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/fs/TempDir.h"
#include "common/utils/IndexKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/BuildTagVectorIndexTask.h"
#include "storage/index/LookupAnnProcessor.h"
#include "storage/index/LookupProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/test/QueryTestUtils.h"
#include "storage/test/TestUtils.h"

using nebula::cpp2::PartitionID;
using nebula::cpp2::PropertyType;
using nebula::cpp2::TagID;
using nebula::storage::cpp2::IndexColumnHint;
using nebula::storage::cpp2::NewVertex;

namespace nebula {
namespace storage {

ObjectPool objPool;
auto pool = &objPool;
int gVectorJobId = 0;
class LookupAnnProcessorTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUp() override {
    manager_ = AdminTaskManager::instance();
    manager_->init();
    FLAGS_query_concurrently = GetParam();
  }

  void TearDown() override {
    manager_->shutdown();
  }
  static AdminTaskManager* manager_;
};
AdminTaskManager* LookupAnnProcessorTest::manager_{nullptr};

TEST_P(LookupAnnProcessorTest, BasicAnnSearchTest) {
  fs::TempDir rootPath("/tmp/BasicAnnSearchTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  GraphSpaceID spaceId = 1;
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

  // Setup test data
  auto* processor = AddVerticesProcessor::instance(env, nullptr);

  LOG(INFO) << "Build AddVectorVerticesRequest...";
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVectorVerticesReq();

  LOG(INFO) << "Test AddVectorVerticesProcessor...";
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  LOG(INFO) << "Check vector data in kv store...";
  // The number of vertices is 84
  checkAddVerticesData(req, env, 84, 0);

  // create ann index
  {
    cpp2::TaskPara parameter;
    parameter.space_id_ref() = 1;
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.parts_ref() = parts;
    parameter.task_specific_paras_ref() = {
        "6",
    };  // [index_id]

    cpp2::AddTaskRequest request;
    request.job_type_ref() = meta::cpp2::JobType::BUILD_TAG_VECTOR_INDEX;
    request.job_id_ref() = ++gVectorJobId;
    request.task_id_ref() = 21;
    request.para_ref() = std::move(parameter);

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<BuildTagVectorIndexTask>(env, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    } while (!manager_->isFinished(gVectorJobId - 1, 21));
    // Check the vertex data count
    LOG(INFO) << "Check build tag vector index...";
    // Check the vector index data count (id-vid mapping)
    int indexDataNum = 0;
    for (auto part : parts) {
      auto prefix = NebulaKeyUtils::idVidTagPrefix(part, 6);  // Vector index ID is 6
      std::unique_ptr<kvstore::KVIterator> iter;
      auto ret = env->kvstore_->prefix(1, part, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
      while (iter && iter->valid()) {
        indexDataNum++;
        iter->next();
      }
    }
    // The number of index entries should equal vector vertices count
    EXPECT_EQ(84, indexDataNum);

    indexDataNum = 0;
    for (auto part : parts) {
      auto prefix = NebulaKeyUtils::vidIdTagPrefix(part, 6);  // Vector index ID is 6
      std::unique_ptr<kvstore::KVIterator> iter;
      auto ret = env->kvstore_->prefix(1, part, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
      while (iter && iter->valid()) {
        indexDataNum++;
        iter->next();
      }
    }
    EXPECT_EQ(84, indexDataNum);
  }
  /**
   * Basic ANN search test
   * lookup plan should be:
   *              +--------+---------+
   *              |       Plan       |
   *              +--------+---------+
   *                       |
   *              +--------+---------+
   *              |    DeDupNode     |
   *              +--------+---------+
   *                       |
   *            +----------+-----------+
   *            +   IndexOutputNode    +
   *            +----------+-----------+
   *                       |
   *            +----------+-----------+
   *            +   AnnIndexScanNode   +
   *            +----------+-----------+
   **/
  {
    auto* annProcessor = LookupAnnProcessor::instance(env, &kLookupCounters, threadPool.get());
    cpp2::LookupAnnIndexRequest annReq;
    // tag4 is vector tag, index 6 is ann index
    annReq.space_id_ref() = spaceId;
    std::vector<PartitionID> parts{1, 2, 3, 4, 5, 6};
    annReq.parts_ref() = std::move(parts);

    // Set up ANN index spec
    cpp2::AnnIndexSpec annSpec;
    cpp2::IndexQueryContext context;
    context.filter_ref() = "";
    context.index_id_ref() = 6;  // ANN index id
    annSpec.context_ref() = std::move(context);

    // Add schema IDs
    std::vector<nebula::cpp2::SchemaID> schemaIds;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 4;
    schemaIds.emplace_back(std::move(schemaId));
    annSpec.schema_ids_ref() = std::move(schemaIds);

    annReq.ann_indice_ref() = std::move(annSpec);
    annReq.limit_ref() = 3;
    annReq.param_ref() = 50;  // Search parameter

    // Create query vector
    auto queryVec = std::vector<float>{1.0f, 2.0f, 3.0f};
    annReq.query_vector_ref() = Value(Vector(queryVec));

    // Set return columns
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kDis);
    annReq.return_columns_ref() = std::move(returnCols);

    auto annFut = annProcessor->getFuture();
    annProcessor->process(annReq);
    auto annResp = std::move(annFut).get();

    // Check response
    EXPECT_TRUE(annResp.get_data() != nullptr);
    EXPECT_EQ(0, (*(*annResp.result_ref()).failed_parts_ref()).size());
    auto columns = (*annResp.data_ref()).colNames;
    LOG(ERROR) << "Resp Columns: " << folly::join(", ", columns);
    auto actualRows = (*annResp.data_ref()).rows;
    LOG(INFO) << "ANN search returned " << annResp.get_data()->size() << " results";
    for (const auto& row : actualRows) {
      LOG(ERROR) << "Resp Row: " << row;
    }
  }
  std::filesystem::remove_all(rootPath.path());
  std::filesystem::remove_all("/home/lzy/nebula/build/testvector");
}

TEST_P(LookupAnnProcessorTest, BasicHNSWAnnSearchTest) {
  fs::TempDir rootPath("/tmp/BasicHNSWAnnSearchTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  GraphSpaceID spaceId = 1;
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

  // Setup test data
  auto* processor = AddVerticesProcessor::instance(env, nullptr);

  LOG(INFO) << "Build AddVectorVerticesRequest...";
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVectorVerticesReq();

  LOG(INFO) << "Test AddVectorVerticesProcessor...";
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  LOG(INFO) << "Check vector data in kv store...";
  // The number of vertices is 84
  checkAddVerticesData(req, env, 84, 0);

  // create ann index
  {
    cpp2::TaskPara parameter;
    parameter.space_id_ref() = 1;
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.parts_ref() = parts;
    parameter.task_specific_paras_ref() = {
        "8",
    };  // [index_id]

    cpp2::AddTaskRequest request;
    request.job_type_ref() = meta::cpp2::JobType::BUILD_TAG_VECTOR_INDEX;
    request.job_id_ref() = ++gVectorJobId;
    request.task_id_ref() = 23;  // Different task ID for HNSW test
    request.para_ref() = std::move(parameter);

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<BuildTagVectorIndexTask>(env, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    } while (!manager_->isFinished(gVectorJobId - 1, 23));
    // Check the vertex data count
    LOG(INFO) << "Check build tag HNSW vector index...";
    // Check the vector index data count (id-vid mapping)
    int indexDataNum = 0;
    for (auto part : parts) {
      auto prefix = NebulaKeyUtils::idVidTagPrefix(part, 8);  // Vector index ID is 8
      std::unique_ptr<kvstore::KVIterator> iter;
      auto ret = env->kvstore_->prefix(1, part, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
      while (iter && iter->valid()) {
        indexDataNum++;
        iter->next();
      }
    }
    // The number of index entries should equal vector vertices count
    EXPECT_EQ(84, indexDataNum);

    indexDataNum = 0;
    for (auto part : parts) {
      auto prefix = NebulaKeyUtils::vidIdTagPrefix(part, 8);  // Vector index ID is 8
      std::unique_ptr<kvstore::KVIterator> iter;
      auto ret = env->kvstore_->prefix(1, part, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
      while (iter && iter->valid()) {
        indexDataNum++;
        iter->next();
      }
    }
    EXPECT_EQ(84, indexDataNum);
  }
  /**
   * Basic HNSW ANN search test
   * lookup plan should be:
   *              +--------+---------+
   *              |       Plan       |
   *              +--------+---------+
   *                       |
   *              +--------+---------+
   *              |    DeDupNode     |
   *              +--------+---------+
   *                       |
   *            +----------+-----------+
   *            +   IndexOutputNode    +
   *            +----------+-----------+
   *                       |
   *            +----------+-----------+
   *            +   AnnIndexScanNode   +
   *            +----------+-----------+
   **/
  {
    auto* annProcessor = LookupAnnProcessor::instance(env, &kLookupCounters, threadPool.get());
    cpp2::LookupAnnIndexRequest annReq;
    // tag4 is vector tag, index 8 is ann index
    annReq.space_id_ref() = spaceId;
    std::vector<PartitionID> parts{1, 2, 3, 4, 5, 6};
    annReq.parts_ref() = std::move(parts);

    // Set up ANN index spec
    cpp2::AnnIndexSpec annSpec;
    cpp2::IndexQueryContext context;
    context.filter_ref() = "";
    context.index_id_ref() = 8;  // ANN index id
    annSpec.context_ref() = std::move(context);

    // Add schema IDs
    std::vector<nebula::cpp2::SchemaID> schemaIds;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 4;
    schemaIds.emplace_back(std::move(schemaId));
    annSpec.schema_ids_ref() = std::move(schemaIds);

    annReq.ann_indice_ref() = std::move(annSpec);
    annReq.limit_ref() = 3;
    annReq.param_ref() = 50;  // Search parameter (efSearch for HNSW)

    // Create query vector
    auto queryVec = std::vector<float>{1.0f, 2.0f, 3.0f};
    annReq.query_vector_ref() = Value(Vector(queryVec));

    // Set return columns
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kDis);
    annReq.return_columns_ref() = std::move(returnCols);

    auto annFut = annProcessor->getFuture();
    annProcessor->process(annReq);
    auto annResp = std::move(annFut).get();

    // Check response
    EXPECT_TRUE(annResp.get_data() != nullptr);
    EXPECT_EQ(0, (*(*annResp.result_ref()).failed_parts_ref()).size());
    auto columns = (*annResp.data_ref()).colNames;
    LOG(ERROR) << "HNSW Resp Columns: " << folly::join(", ", columns);
    auto actualRows = (*annResp.data_ref()).rows;
    LOG(INFO) << "HNSW ANN search returned " << annResp.get_data()->size() << " results";
    for (const auto& row : actualRows) {
      LOG(ERROR) << "HNSW Resp Row: " << row;
    }
  }
  std::filesystem::remove_all(rootPath.path());
  std::filesystem::remove_all("/home/lzy/nebula/build/testvector");
}

INSTANTIATE_TEST_SUITE_P(LookupAnnProcessorTest, LookupAnnProcessorTest, ::testing::Bool());

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
