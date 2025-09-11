/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/NebulaKeyUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/VectorIndexManager.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/BuildEdgeVectorIndexTask.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/test/ChainTestUtils.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

int gVectorJobId = 0;
struct VectorEdge {
  std::string srcId;
  std::string dstId;
};

class BuildEdgeVectorIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    LOG(INFO) << "SetUp BuildEdgeVectorIndexTest TestCase";
    manager_ = AdminTaskManager::instance();
    manager_->init();
  }

  static void TearDownTestCase() {
    LOG(INFO) << "TearDown BuildEdgeVectorIndexTest TestCase";
    manager_->shutdown();
  }

  void SetUp() override {
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/BuildEdgeVectorIndexTest.XXXXXX");
    cluster_ = std::make_unique<nebula::mock::MockCluster>();
    cluster_->initStorageKV(rootPath_->path());
    env_ = cluster_->storageEnv_.get();
  }

  void TearDown() override {
    boost::filesystem::remove_all(rootPath_->path());
    cluster_.reset();
    rootPath_.reset();
  }

  static StorageEnv* env_;
  static AdminTaskManager* manager_;

  nebula::cpp2::ErrorCode getVidByVectorId(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           IndexID indexId,
                                           VectorID vectorId,
                                           std::string& srcId,
                                           std::string& dstId) {
    auto vidIdKey = NebulaKeyUtils::vidIdEdgePrefix(partId, indexId, vectorId);
    std::string val;
    auto ret = env_->kvstore_->get(spaceId, partId, vidIdKey, &val);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Failed to get vid by vectorId: " << vectorId;
      return ret;
    }
    auto vidLen = env_->schemaMan_->getSpaceVidLen(1).value();
    srcId = NebulaKeyUtils::getVectorSrcId(vidLen, val);
    dstId = NebulaKeyUtils::getVectorDstId(vidLen, val);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 private:
  static std::unique_ptr<fs::TempDir> rootPath_;
  static std::unique_ptr<nebula::mock::MockCluster> cluster_;
};

StorageEnv* BuildEdgeVectorIndexTest::env_{nullptr};
AdminTaskManager* BuildEdgeVectorIndexTest::manager_{nullptr};
std::unique_ptr<fs::TempDir> BuildEdgeVectorIndexTest::rootPath_{nullptr};
std::unique_ptr<nebula::mock::MockCluster> BuildEdgeVectorIndexTest::cluster_{nullptr};

// Test building edge vector index and checking all data
TEST_F(BuildEdgeVectorIndexTest, BuildEdgeVectorIndexCheckAllData) {
  // Add Vector Edges
  {
    auto* processor = AddEdgesProcessor::instance(BuildEdgeVectorIndexTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddVectorEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    LOG(INFO) << "Check data in kv store...";
    // The number of data in serve is 168
    checkAddVectorEdgesData(req, env_, 168, 0);
  }

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {
      "7",
  };  // [index_id] - Edge vector index ID is 7

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::BUILD_EDGE_VECTOR_INDEX;
  request.job_id_ref() = ++gVectorJobId;
  request.task_id_ref() = 22;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<BuildEdgeVectorIndexTask>(BuildEdgeVectorIndexTest::env_,
                                                         std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(gVectorJobId - 1, 22));

  // Check the edge data count
  LOG(INFO) << "Check build edge vector index...";
  // Check the vector index data count (id-vid mapping)
  int indexDataNum = 0;
  for (auto part : parts) {
    auto prefix = NebulaKeyUtils::idVidEdgePrefix(part, 7);  // Vector index ID is 7
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = BuildEdgeVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  // The number of index entries should equal vector edges count
  EXPECT_EQ(84, indexDataNum);  // Assuming same count as edge vectors

  indexDataNum = 0;
  for (auto part : parts) {
    auto prefix = NebulaKeyUtils::vidIdEdgePrefix(part, 7);  // Vector index ID is 7
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = BuildEdgeVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  // The number of index entries should equal vector edges count
  EXPECT_EQ(84, indexDataNum);  // Assuming same count as edge vectors
  // Check ANN index is created
  for (auto part : parts) {
    auto annIdx = env_->annIndexMan_->getIndex(1, part, 7);
    if (annIdx.ok()) {
      LOG(INFO) << "Part " << part << " ANN index found";
    }
  }

  // Check IVF ANN index is created
  std::vector<std::pair<VectorEdge, float>> vids;

  for (auto part : parts) {
    auto annIdx = env_->annIndexMan_->getIndex(1, part, 7);
    if (!annIdx.ok()) {
      LOG(INFO) << "Part " << part << " IVF ANN index not found";
    }

    auto query = std::vector<float>{1.0f, 2.0f, 3.0f};
    SearchResult res;
    // Use IVF search parameters
    auto searchParam = SearchParamsIVF(3, query.data(), 3, 50);  // efSearch = 50
    auto ret = annIdx.value()->search(&searchParam, &res);
    if (!ret.ok()) {
      LOG(INFO) << "Part " << part << " IVF ANN index search failed";
    } else {
      EXPECT_EQ(3, res.IDs.size());
      EXPECT_EQ(3, res.distances.size());
      for (size_t i = 0; i < res.IDs.size(); i++) {
        std::string srcId, dstId;
        auto rc = getVidByVectorId(1, part, 7, res.IDs[i], srcId, dstId);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, rc);
        VectorEdge edge{srcId, dstId};
        vids.emplace_back(std::make_pair(edge, res.distances[i]));
      }
    }
  }

  std::sort(
      vids.begin(), vids.end(), [](const auto& a, const auto& b) { return a.second < b.second; });

  EXPECT_EQ(18, vids.size());
  for (const auto& vid : vids) {
    LOG(INFO) << "IVF SrcId: " << vid.first.srcId << ", DstId: " << vid.first.dstId
              << ", Distance: " << vid.second;
  }
}

TEST_F(BuildEdgeVectorIndexTest, BuildEdgeHNSWVectorIndexCheckAllData) {
  // Add Vector Edges
  {
    auto* processor = AddEdgesProcessor::instance(BuildEdgeVectorIndexTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddVectorEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    LOG(INFO) << "Check data in kv store...";
    // The number of data in serve is 168
    checkAddVectorEdgesData(req, env_, 168, 0);
  }

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {
      "9",
  };  // [index_id] - Edge vector index ID is 9

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::BUILD_EDGE_VECTOR_INDEX;
  request.job_id_ref() = ++gVectorJobId;
  request.task_id_ref() = 23;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<BuildEdgeVectorIndexTask>(BuildEdgeVectorIndexTest::env_,
                                                         std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(gVectorJobId - 1, 23));

  // Check the edge data count
  LOG(INFO) << "Check build edge vector index...";
  // Check the vector index data count (id-vid mapping)
  int indexDataNum = 0;
  for (auto part : parts) {
    auto prefix = NebulaKeyUtils::idVidEdgePrefix(part, 9);  // Vector index ID is 9
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = BuildEdgeVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  // The number of index entries should equal vector edges count
  EXPECT_EQ(84, indexDataNum);  // Assuming same count as edge vectors

  indexDataNum = 0;
  for (auto part : parts) {
    auto prefix = NebulaKeyUtils::vidIdEdgePrefix(part, 9);  // Vector index ID is 9
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = BuildEdgeVectorIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  // The number of index entries should equal vector edges count
  EXPECT_EQ(84, indexDataNum);  // Assuming same count as edge vectors
  // Check ANN index is created
  auto& vecIdxMgr = VectorIndexManager::getInstance();
  for (auto part : parts) {
    auto annIdx = vecIdxMgr.getIndex(1, part, 9);
    if (annIdx.ok()) {
      LOG(INFO) << "Part " << part << " ANN index found";
    }
  }

  std::vector<std::pair<VectorEdge, float>> vids;

  for (auto part : parts) {
    auto annIdx = env_->annIndexMan_->getIndex(1, part, 9);
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
        std::string srcId, dstId;
        auto rc = getVidByVectorId(1, part, 9, res.IDs[i], srcId, dstId);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, rc);
        VectorEdge edge{srcId, dstId};
        vids.emplace_back(std::make_pair(edge, res.distances[i]));
      }
    }
  }

  std::sort(
      vids.begin(), vids.end(), [](const auto& a, const auto& b) { return a.second < b.second; });

  EXPECT_EQ(18, vids.size());
  for (const auto& vid : vids) {
    LOG(INFO) << "HNSW SrcId: " << vid.first.srcId << ", DstId: " << vid.first.dstId
              << ", Distance: " << vid.second;
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
