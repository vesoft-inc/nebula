/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/OperationKeyUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

int gJobId = 0;

class RebuildIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    LOG(INFO) << "SetUp RebuildIndexTest TestCase";
    manager_ = AdminTaskManager::instance();
    manager_->init();
  }

  static void TearDownTestCase() {
    LOG(INFO) << "TearDown RebuildIndexTest TestCase";
    manager_->shutdown();
  }

  void SetUp() override {
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/RebuildIndexTest.XXXXXX");
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

 private:
  static std::unique_ptr<fs::TempDir> rootPath_;
  static std::unique_ptr<nebula::mock::MockCluster> cluster_;
};

StorageEnv* RebuildIndexTest::env_{nullptr};
AdminTaskManager* RebuildIndexTest::manager_{nullptr};
std::unique_ptr<fs::TempDir> RebuildIndexTest::rootPath_{nullptr};
std::unique_ptr<nebula::mock::MockCluster> RebuildIndexTest::cluster_{nullptr};

// Check data after rebuild index, then delete data
TEST_F(RebuildIndexTest, RebuildTagIndexCheckALLData) {
  // Add Vertices
  {
    auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {"4", "5"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 13;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Check the data count
  LOG(INFO) << "Check rebuild tag index...";
  auto vidSizeRet = RebuildIndexTest::env_->schemaMan_->getSpaceVidLen(1);
  auto vidSize = vidSizeRet.value();
  EXPECT_LT(0, vidSize);
  int dataNum = 0;
  for (auto part : parts) {
    auto prefix = NebulaKeyUtils::tagPrefix(part);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = RebuildIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      dataNum++;
      iter->next();
    }
  }
  // The number of vertices is 81, the count of players_ and teams_
  EXPECT_EQ(81, dataNum);

  // Check the index data count
  int indexDataNum = 0;
  for (auto part : parts) {
    auto prefix = IndexKeyUtils::indexPrefix(part);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = RebuildIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  // The number of vertices index is 162, the count of players_ and teams_
  EXPECT_EQ(162, indexDataNum);

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  sleep(1);

  // Delete vertices
  {
    auto* processor = DeleteVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);

    LOG(INFO) << "Build DeleteVerticesRequest...";
    cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

    LOG(INFO) << "Test DeleteVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }
}

// Check data after rebuild index, then delete data
TEST_F(RebuildIndexTest, RebuildEdgeIndexCheckALLData) {
  // Add Edges
  {
    auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = parts;
  parameter.task_specific_paras_ref() = {"103", "104"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_EDGE_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 16;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);
  auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Check the data count
  LOG(INFO) << "Check rebuild edge index...";
  auto vidSizeRet = RebuildIndexTest::env_->schemaMan_->getSpaceVidLen(1);
  auto vidSize = vidSizeRet.value();
  EXPECT_LT(0, vidSize);
  int dataNum = 0;
  for (auto part : parts) {
    auto prefix = NebulaKeyUtils::edgePrefix(part);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = RebuildIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      if (NebulaKeyUtils::isEdge(vidSize, iter->key())) {
        dataNum++;
      }
      iter->next();
    }
  }
  // The number of edges is 334, only serves_ count
  EXPECT_EQ(334, dataNum);

  // Check the index data count
  int indexDataNum = 0;
  for (auto part : parts) {
    auto prefix = IndexKeyUtils::indexPrefix(part);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = RebuildIndexTest::env_->kvstore_->prefix(1, part, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    while (iter && iter->valid()) {
      indexDataNum++;
      iter->next();
    }
  }
  // The number of edges index is 334, only positive side count of serves_
  EXPECT_EQ(334, indexDataNum);

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  sleep(1);

  // Delete edges
  {
    auto* processor = DeleteEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
    LOG(INFO) << "Build DeleteEdgesRequest...";
    cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();

    LOG(INFO) << "Test DeleteEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }
}

TEST_F(RebuildIndexTest, RebuildTagIndexWithDelete) {
  auto writer = std::make_unique<thread::GenericWorker>();
  EXPECT_TRUE(writer->start());

  auto deleteVertices = [&]() mutable {
    LOG(INFO) << "Start Background Writer & Delete is Running";
    cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();
    auto* processor = DeleteVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  };

  // Add Vertices
  auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = std::move(parts);
  parameter.task_specific_paras_ref() = {"4", "5"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 11;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  writer->addTask(deleteVertices).get();

  auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  LOG(INFO) << "Check rebuild tag index...";
  for (auto& key : mock::MockData::mockPlayerIndexKeys()) {
    std::string value;
    auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Check Key " << key.first << " " << key.second;
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, code);
  }

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  writer->stop();
  sleep(1);
}

TEST_F(RebuildIndexTest, RebuildTagIndexWithAppend) {
  auto writer = std::make_unique<thread::GenericWorker>();
  EXPECT_TRUE(writer->start());

  auto appendVertices = [&]() mutable {
    LOG(INFO) << "Start Background Writer & Append is Running";
    auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    std::move(fut).get();
  };

  // Add Vertices
  auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq(true);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = std::move(parts);
  parameter.task_specific_paras_ref() = {"4", "5"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 12;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);
  writer->addTask(appendVertices).get();

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  LOG(INFO) << "Check rebuild tag index...";
  for (auto& key : mock::MockData::mockPlayerIndexKeys(true)) {
    std::string value;
    auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  }

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  writer->stop();

  for (int i = 1; i <= 5; ++i) {
    LOG(INFO) << "sleep for " << i << "s";
    sleep(1);
  }
}

TEST_F(RebuildIndexTest, RebuildTagIndex) {
  // Add Vertices
  auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = std::move(parts);

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 13;
  parameter.task_specific_paras_ref() = {"4", "5"};
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    // usleep(50);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Check the result
  LOG(INFO) << "Check rebuild tag index...";
  for (auto& key : mock::MockData::mockPlayerIndexKeys()) {
    std::string value;
    auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  }

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  for (int i = 1; i <= 5; ++i) {
    LOG(INFO) << "sleep for " << i << "s";
    sleep(1);
  }
}

TEST_F(RebuildIndexTest, RebuildEdgeIndexWithDelete) {
  auto writer = std::make_unique<thread::GenericWorker>();
  EXPECT_TRUE(writer->start());

  auto deleteEdges = [&]() mutable {
    cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();
    auto* processor = DeleteEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    std::move(fut).get();
  };

  // Add Edges
  auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = std::move(parts);
  parameter.task_specific_paras_ref() = {"103", "104"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_EDGE_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 14;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);

  writer->addTask(deleteEdges).get();

  auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    // usleep(50);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Check the result
  LOG(INFO) << "Check rebuild edge index...";
  for (auto& key : mock::MockData::mockServeIndexKeys()) {
    std::string value;
    auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Check Key " << key.first << " " << key.second;
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, code);
  }

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  writer->stop();
  // sleep(1);
  for (int i = 1; i <= 5; ++i) {
    LOG(INFO) << "sleep for " << i << "s";
    sleep(1);
  }
}

TEST_F(RebuildIndexTest, RebuildEdgeIndexWithAppend) {
  auto writer = std::make_unique<thread::GenericWorker>();
  EXPECT_TRUE(writer->start());

  auto appendEdges = [&]() mutable {
    auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(true);
    auto fut = processor->getFuture();
    processor->process(req);
    std::move(fut).get();
  };
  writer->addTask(appendEdges);

  // Add Edges
  auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = std::move(parts);
  parameter.task_specific_paras_ref() = {"103", "104"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_EDGE_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 15;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);
  auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Check the result
  LOG(INFO) << "Check rebuild tag index...";
  for (auto& key : mock::MockData::mockServeIndexKeys()) {
    std::string value;
    auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  }

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  writer->stop();
  sleep(1);
  for (int i = 1; i <= 5; ++i) {
    LOG(INFO) << "sleep for " << i << "s";
    sleep(1);
  }
}

// Regression test for multi-batch rebuild index replay.
// Verifies:
// 1. Multiple batches are flushed within a single round when operation logs exceed batch_size
// 2. The remainder batch after all flushes is correctly handled
// 3. totalBytesThisRound accumulates across flushes so the round converges (no infinite loop)
// 4. Operation logs are properly cleaned up after replay
TEST_F(RebuildIndexTest, RebuildIndexMultiBatchReplay) {
  // Save original batch size and set a small value to force multiple flushes
  auto originalBatchSize = FLAGS_rebuild_index_batch_size;
  FLAGS_rebuild_index_batch_size = 128;  // 128 bytes per batch

  GraphSpaceID space = 1;
  PartitionID part = 1;

  // Write enough operation logs to trigger multiple flushes
  // Each modify operation key is ~20+ bytes, so 20 operations should be ~400+ bytes
  // With batch_size=128, this triggers at least 3 flushes + a remainder
  const int numOps = 20;
  {
    std::vector<std::string> opKeys;
    for (int i = 0; i < numOps; ++i) {
      std::string keyData = "test_key_" + std::to_string(i) + "_padding_data";
      std::string opKey = OperationKeyUtils::modifyOperationKey(part, keyData);
      opKeys.push_back(std::move(keyData));

      std::string opVal = "test_value_" + std::to_string(i);
      std::vector<kvstore::KV> kvs;
      kvs.emplace_back(std::move(opKey), opVal);

      folly::Baton<true, std::atomic> baton;
      auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
      RebuildIndexTest::env_->kvstore_->asyncMultiPut(space, part, std::move(kvs),
          [&result, &baton](nebula::cpp2::ErrorCode code) {
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
              result = code;
            }
            baton.post();
          });
      baton.wait();
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    }

    // Verify operation logs are written
    {
      std::unique_ptr<kvstore::KVIterator> iter;
      auto prefix = OperationKeyUtils::operationPrefix(part);
      auto ret = RebuildIndexTest::env_->kvstore_->prefix(space, part, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
      int count = 0;
      while (iter && iter->valid()) {
        count++;
        iter->next();
      }
      EXPECT_EQ(numOps, count);
    }
  }

  // Create a rebuild tag index task and run it
  cpp2::TaskPara parameter;
  parameter.space_id_ref() = space;
  parameter.parts_ref() = {part};
  parameter.task_specific_paras_ref() = {"4"};  // tag index id

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 20;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);
  auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task to finish
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Verify operation logs are cleaned up after replay
  {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = OperationKeyUtils::operationPrefix(part);
    auto ret = RebuildIndexTest::env_->kvstore_->prefix(space, part, prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      int count = 0;
      while (iter && iter->valid()) {
        count++;
        iter->next();
      }
      // All operation logs should be cleaned up
      EXPECT_EQ(0, count);
    }
  }

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();

  // Restore original batch size
  FLAGS_rebuild_index_batch_size = originalBatchSize;
}

// Test that totalBytesThisRound converges correctly when operation logs
// span multiple batches. Even if the final remainder is small, the
// accumulated total should exceed batch_size, preventing the round from
// entering LOCKED state and causing an infinite loop.
TEST_F(RebuildIndexTest, RebuildIndexRoundConvergence) {
  auto originalBatchSize = FLAGS_rebuild_index_batch_size;
  // Set batch size so that 3 operations trigger one flush, leaving 1 as remainder
  FLAGS_rebuild_index_batch_size = 256;

  GraphSpaceID space = 1;
  PartitionID part = 2;

  // Write 4 operation logs: 3 will trigger a flush, 1 will be the remainder
  // totalBytesThisRound = flushed_bytes + remainder_bytes > batch_size
  // So the round should NOT enter LOCKED state
  const int numOps = 4;
  {
    for (int i = 0; i < numOps; ++i) {
      std::string keyData = "convergence_test_key_" + std::to_string(i) + "_extra_padding_for_size";
      std::string opKey = OperationKeyUtils::modifyOperationKey(part, keyData);
      std::string opVal = "convergence_value_" + std::to_string(i);
      std::vector<kvstore::KV> kvs;
      kvs.emplace_back(std::move(opKey), opVal);

      folly::Baton<true, std::atomic> baton;
      auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
      RebuildIndexTest::env_->kvstore_->asyncMultiPut(space, part, std::move(kvs),
          [&result, &baton](nebula::cpp2::ErrorCode code) {
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
              result = code;
            }
            baton.post();
          });
      baton.wait();
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    }
  }

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = space;
  parameter.parts_ref() = {part};
  parameter.task_specific_paras_ref() = {"4"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_TAG_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 21;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);
  auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait with timeout to detect infinite loop
  int waitCount = 0;
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    waitCount++;
    if (waitCount > 200) {  // 10 second timeout
      FAIL() << "Rebuild index task did not finish, possible infinite loop in replay";
    }
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Verify operation logs are cleaned up
  {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = OperationKeyUtils::operationPrefix(part);
    auto ret = RebuildIndexTest::env_->kvstore_->prefix(space, part, prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      int count = 0;
      while (iter && iter->valid()) {
        count++;
        iter->next();
      }
      EXPECT_EQ(0, count);
    }
  }

  RebuildIndexTest::env_->rebuildIndexGuard_->clear();
  FLAGS_rebuild_index_batch_size = originalBatchSize;
}

TEST_F(RebuildIndexTest, RebuildEdgeIndex) {
  // Add Edges
  auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  cpp2::TaskPara parameter;
  parameter.space_id_ref() = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
  parameter.parts_ref() = std::move(parts);
  parameter.task_specific_paras_ref() = {"103", "104"};

  cpp2::AddTaskRequest request;
  request.job_type_ref() = meta::cpp2::JobType::REBUILD_EDGE_INDEX;
  request.job_id_ref() = ++gJobId;
  request.task_id_ref() = 16;
  request.para_ref() = std::move(parameter);

  auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&) {};
  TaskContext context(request, callback);
  auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
  manager_->addAsyncTask(task);

  // Wait for the task finished
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  } while (!manager_->isFinished(context.jobId_, context.taskId_));

  // Check the result
  LOG(INFO) << "Check rebuild edge index...";
  for (auto& key : mock::MockData::mockServeIndexKeys()) {
    std::string value;
    auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  }
  for (int i = 1; i <= 5; ++i) {
    LOG(INFO) << "sleep for " << i << "s";
    sleep(1);
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
