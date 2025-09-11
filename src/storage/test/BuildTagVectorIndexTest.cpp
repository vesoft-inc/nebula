/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/thrift/ThriftTypes.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/vectorIndex/VectorIndexUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/VectorIndexManager.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/BuildTagVectorIndexTask.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/test/ChainTestUtils.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

int gVectorJobId = 0;

class BuildTagVectorIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    LOG(INFO) << "SetUp BuildTagVectorIndexTest TestCase";
    manager_ = AdminTaskManager::instance();
    manager_->init();
  }

  static void TearDownTestCase() {
    LOG(INFO) << "TearDown BuildTagVectorIndexTest TestCase";
    manager_->shutdown();
  }

  void SetUp() override {
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/BuildTagVectorIndexTest.XXXXXX");
    cluster_ = std::make_unique<nebula::mock::MockCluster>();
    cluster_->initStorageKV(rootPath_->path());
    env_ = cluster_->storageEnv_.get();
  }

  void TearDown() override {
    cluster_.reset();
    rootPath_.reset();
  }

  static StorageEnv* env_;
  static AdminTaskManager* manager_;

  nebula::cpp2::ErrorCode getVidByVectorId(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           IndexID indexId,
                                           VectorID vectorId,
                                           std::string& vid) {
    auto vidIdKey = NebulaKeyUtils::vidIdTagPrefix(partId, indexId, vectorId);
    LOG(ERROR) << "Vid-id key: " << folly::hexlify(vidIdKey);
    auto ret = env_->kvstore_->get(spaceId, partId, vidIdKey, &vid);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Failed to get vid by vectorId: " << vectorId;
      return ret;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 private:
  static std::unique_ptr<fs::TempDir> rootPath_;
  static std::unique_ptr<nebula::mock::MockCluster> cluster_;
};

StorageEnv* BuildTagVectorIndexTest::env_{nullptr};
AdminTaskManager* BuildTagVectorIndexTest::manager_{nullptr};
std::unique_ptr<fs::TempDir> BuildTagVectorIndexTest::rootPath_{nullptr};
std::unique_ptr<nebula::mock::MockCluster> BuildTagVectorIndexTest::cluster_{nullptr};

// Test building vector index and checking all data
TEST_F(BuildTagVectorIndexTest, BuildTagVectorIndexCheckAllData) {
  // Add Vector Vertices
  {
    auto* processor = AddVerticesProcessor::instance(BuildTagVectorIndexTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVectorVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

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

  auto task =
      std::make_shared<BuildTagVectorIndexTask>(BuildTagVectorIndexTest::env_, std::move(context));
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
    auto ret = BuildTagVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
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
    auto ret = BuildTagVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  EXPECT_EQ(84, indexDataNum);
  // Check ANN index is created
  std::vector<std::pair<std::string, float>> vids;

  for (auto part : parts) {
    auto annIdx = env_->annIndexMan_->getIndex(1, part, 6);
    if (!annIdx.ok()) {
      LOG(INFO) << "Part " << part << " ANN index not found";
    }

    auto query = std::vector<float>{1.0f, 2.0f, 3.0f};
    SearchResult res;
    auto searchParam = SearchParamsIVF(3, query.data(), 3, 3);
    auto ret = annIdx.value()->search(&searchParam, &res);
    if (!ret.ok()) {
      LOG(INFO) << "Part " << part << " ANN index search failed";
    } else {
      EXPECT_EQ(3, res.IDs.size());
      EXPECT_EQ(3, res.distances.size());
      for (size_t i = 0; i < res.IDs.size(); i++) {
        std::string vid;
        auto rc = getVidByVectorId(1, part, 6, res.IDs[i], vid);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, rc);
        vids.emplace_back(std::make_pair(vid, res.distances[i]));
      }
    }
  }

  std::sort(
      vids.begin(), vids.end(), [](const auto& a, const auto& b) { return a.second < b.second; });

  EXPECT_EQ(18, vids.size());
  for (size_t i = 0; i < vids.size(); i++) {
    LOG(INFO) << "VectorID: " << vids[i].first << ", Distance: " << vids[i].second;
  }
}

// Test building HNSW vector index and checking all data
TEST_F(BuildTagVectorIndexTest, BuildTagHNSWVectorIndexCheckAllData) {
  // Add Vector Vertices
  {
    auto* processor = AddVerticesProcessor::instance(BuildTagVectorIndexTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVectorVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

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
  request.task_id_ref() = 22;  // Different task ID for HNSW test
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task =
      std::make_shared<BuildTagVectorIndexTask>(BuildTagVectorIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(gVectorJobId - 1, 22));

  // Check the vertex data count
  LOG(INFO) << "Check build tag HNSW vector index...";
  // Check the vector index data count (id-vid mapping)
  int indexDataNum = 0;
  for (auto part : parts) {
    auto prefix = NebulaKeyUtils::idVidTagPrefix(part, 8);  // Vector index ID is 8
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = BuildTagVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
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
    auto ret = BuildTagVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  EXPECT_EQ(84, indexDataNum);

  // Check HNSW ANN index is created
  std::vector<std::pair<std::string, float>> vids;

  for (auto part : parts) {
    auto annIdx = env_->annIndexMan_->getIndex(1, part, 8);
    if (!annIdx.ok()) {
      LOG(INFO) << "Part " << part << " HNSW ANN index not found";
    }

    auto query = std::vector<float>{1.0f, 2.0f, 3.0f};
    SearchResult res;
    // Use HNSW search parameters instead of IVF
    auto searchParam = SearchParamsHNSW(3, query.data(), 3, 50);  // efSearch = 50
    auto ret = annIdx.value()->search(&searchParam, &res);
    if (!ret.ok()) {
      LOG(INFO) << "Part " << part << " HNSW ANN index search failed";
    } else {
      EXPECT_EQ(3, res.IDs.size());
      EXPECT_EQ(3, res.distances.size());
      for (size_t i = 0; i < res.IDs.size(); i++) {
        std::string vid;
        auto rc = getVidByVectorId(1, part, 8, res.IDs[i], vid);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, rc);
        vids.emplace_back(std::make_pair(vid, res.distances[i]));
      }
    }
  }

  std::sort(
      vids.begin(), vids.end(), [](const auto& a, const auto& b) { return a.second < b.second; });

  EXPECT_EQ(18, vids.size());
  for (const auto& vid : vids) {
    LOG(INFO) << "HNSW VectorID: " << vid.first << ", Distance: " << vid.second;
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
