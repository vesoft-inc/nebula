/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "interface/gen-cpp2/meta_types.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/StatsTask.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

int gJobId = 0;

class StatsTaskTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    LOG(INFO) << "SetUp StatsTaskTest TestCase";
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/StatsTaskTest.XXXXXX");
    cluster_ = std::make_unique<nebula::mock::MockCluster>();
    cluster_->initStorageKV(rootPath_->path());
    env_ = cluster_->storageEnv_.get();
    manager_ = AdminTaskManager::instance();
    manager_->init();
  }

  static void TearDownTestCase() {
    LOG(INFO) << "TearDown StatsTaskTest TestCase";
    manager_->shutdown();
    cluster_.reset();
    rootPath_.reset();
  }

  void SetUp() override {}

  void TearDown() override {}

  static StorageEnv* env_;
  static AdminTaskManager* manager_;

 private:
  static std::unique_ptr<fs::TempDir> rootPath_;
  static std::unique_ptr<nebula::mock::MockCluster> cluster_;
};

StorageEnv* StatsTaskTest::env_{nullptr};
AdminTaskManager* StatsTaskTest::manager_{nullptr};
std::unique_ptr<fs::TempDir> StatsTaskTest::rootPath_{nullptr};
std::unique_ptr<nebula::mock::MockCluster> StatsTaskTest::cluster_{nullptr};

// Stats data
TEST_F(StatsTaskTest, StatsTagAndEdgeData) {
  // Empty data
  GraphSpaceID spaceId = 1;
  std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};

  {
    cpp2::TaskPara parameter;
    parameter.space_id_ref() = spaceId;
    parameter.parts_ref() = parts;

    cpp2::AddTaskRequest request;
    request.job_type_ref() = meta::cpp2::JobType::STATS;
    request.job_id_ref() = ++gJobId;
    request.task_id_ref() = 13;
    request.para_ref() = std::move(parameter);

    nebula::meta::cpp2::StatsItem statsItem;
    auto callback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatsItem& result) {
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        // Do nothing
      } else {
        if (result.get_status() == nebula::meta::cpp2::JobStatus::FINISHED) {
          statsItem = std::move(result);
        }
      }
    };

    TaskContext context(request, callback);

    auto task = std::make_shared<StatsTask>(StatsTaskTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
      usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Ensure that StatsTask::finish is called.
    for (int i = 0; i < 50; i++) {
      sleep(1);
      if (statsItem.get_status() == nebula::meta::cpp2::JobStatus::FINISHED) {
        break;
      }
    }

    // Check stats result
    ASSERT_EQ(nebula::meta::cpp2::JobStatus::FINISHED, statsItem.get_status());
    // Three tags
    ASSERT_EQ(3, (*statsItem.tag_vertices_ref()).size());
    for (auto& e : *statsItem.tag_vertices_ref()) {
      ASSERT_EQ(0, e.second);
    }

    // Two edgetypes
    ASSERT_EQ(2, (*statsItem.edges_ref()).size());
    for (auto& edge : *statsItem.edges_ref()) {
      ASSERT_EQ(0, edge.second);
    }
    ASSERT_EQ(0, *statsItem.space_vertices_ref());
    ASSERT_EQ(0, *statsItem.space_edges_ref());
  }

  // Add Vertices
  {
    auto* processor = AddVerticesProcessor::instance(StatsTaskTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.space_id_ref() = spaceId;
    parameter.parts_ref() = parts;

    cpp2::AddTaskRequest request;
    request.job_type_ref() = meta::cpp2::JobType::STATS;
    request.job_id_ref() = ++gJobId;
    request.task_id_ref() = 14;
    request.para_ref() = std::move(parameter);

    nebula::meta::cpp2::StatsItem statsItem;
    auto callback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatsItem& result) {
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        // Do nothing
      } else {
        if (result.get_status() == nebula::meta::cpp2::JobStatus::FINISHED) {
          statsItem = std::move(result);
        }
      }
    };

    TaskContext context(request, callback);

    auto task = std::make_shared<StatsTask>(StatsTaskTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
      usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Ensure that StatsTask::finish is called.
    for (int i = 0; i < 50; i++) {
      sleep(1);
      if (statsItem.get_status() == nebula::meta::cpp2::JobStatus::FINISHED) {
        break;
      }
    }

    // Check stats result
    ASSERT_EQ(nebula::meta::cpp2::JobStatus::FINISHED, statsItem.get_status());
    // Three tags
    ASSERT_EQ(3, (*statsItem.tag_vertices_ref()).size());
    for (auto& e : *statsItem.tag_vertices_ref()) {
      if (e.first == "1") {
        ASSERT_EQ(51, e.second);
      } else if (e.first == "2") {
        ASSERT_EQ(30, e.second);
      } else {
        ASSERT_EQ(0, e.second);
      }
    }
    // Two edgetypes
    ASSERT_EQ(2, (*statsItem.edges_ref()).size());
    for (auto& edge : *statsItem.edges_ref()) {
      ASSERT_EQ(0, edge.second);
    }

    ASSERT_EQ(81, *statsItem.space_vertices_ref());
    ASSERT_EQ(0, *statsItem.space_edges_ref());
  }

  // Add Edges
  {
    auto* processor = AddEdgesProcessor::instance(StatsTaskTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.space_id_ref() = spaceId;
    parameter.parts_ref() = parts;

    cpp2::AddTaskRequest request;
    request.job_type_ref() = meta::cpp2::JobType::STATS;
    request.job_id_ref() = ++gJobId;
    request.task_id_ref() = 15;
    request.para_ref() = std::move(parameter);

    nebula::meta::cpp2::StatsItem statsItem;
    auto callback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatsItem& result) {
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        // Do nothing
      } else {
        if (result.get_status() == nebula::meta::cpp2::JobStatus::FINISHED) {
          statsItem = std::move(result);
        }
      }
    };

    TaskContext context(request, callback);

    auto task = std::make_shared<StatsTask>(StatsTaskTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
      usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Ensure that StatsTask::finish is called.
    for (int i = 0; i < 50; i++) {
      sleep(1);
      if (statsItem.get_status() == nebula::meta::cpp2::JobStatus::FINISHED) {
        break;
      }
    }

    // Check stats result
    ASSERT_EQ(nebula::meta::cpp2::JobStatus::FINISHED, statsItem.get_status());
    // Three tags
    ASSERT_EQ(3, (*statsItem.tag_vertices_ref()).size());

    for (auto& e : *statsItem.tag_vertices_ref()) {
      if (e.first == "1") {
        ASSERT_EQ(51, e.second);
      } else if (e.first == "2") {
        ASSERT_EQ(30, e.second);
      } else {
        ASSERT_EQ(0, e.second);
      }
    }

    // Two edgetypes
    ASSERT_EQ(2, (*statsItem.edges_ref()).size());
    for (auto& edge : *statsItem.edges_ref()) {
      if (edge.first == "101") {
        // Do not contain reverse edge datas.
        ASSERT_EQ(167, edge.second);
      } else {
        ASSERT_EQ(0, edge.second);
      }
    }
    // ASSERT_EQ(81, *statsItem.space_vertices_ref());
    EXPECT_EQ(81, *statsItem.space_vertices_ref());
    ASSERT_EQ(167, *statsItem.space_edges_ref());
  }

  // Check the data count
  LOG(INFO) << "Check data in kv store...";
  auto spaceVidLenRet = StatsTaskTest::env_->schemaMan_->getSpaceVidLen(spaceId);
  EXPECT_TRUE(spaceVidLenRet.ok());
  auto spaceVidLen = spaceVidLenRet.value();

  // Check vertex datas and edge datas
  {
    int64_t spaceVertices = 0;
    int64_t spaceEdges = 0;
    std::unordered_map<TagID, int64_t> tagsVertices;
    std::unordered_map<EdgeType, int64_t> edgetypeEdges;
    std::vector<TagID> tags{1, 2, 3};
    std::vector<TagID> edges{101, 102};

    for (auto tag : tags) {
      tagsVertices[tag] = 0;
    }
    for (auto edge : edges) {
      edgetypeEdges[edge] = 0;
    }

    for (auto part : parts) {
      TagID lastTagId = 0;
      VertexID lastVertexId = "";

      VertexID lastSrcVertexId = "";
      EdgeType lastEdgeType = 0;
      VertexID lastDstVertexId = "";
      EdgeRanking lastRank = 0;

      auto prefix = NebulaKeyUtils::tagPrefix(part);
      std::unique_ptr<kvstore::KVIterator> iter;
      auto ret = env_->kvstore_->prefix(spaceId, part, prefix, &iter);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        continue;
      }

      while (iter && iter->valid()) {
        auto key = iter->key();
        auto vId = NebulaKeyUtils::getVertexId(spaceVidLen, key).str();
        auto tagId = NebulaKeyUtils::getTagId(spaceVidLen, key);
        auto it = tagsVertices.find(tagId);
        if (it == tagsVertices.end()) {
          // Invalid data
          iter->next();
          continue;
        }

        if (vId == lastVertexId) {
          if (tagId == lastTagId) {
            // Multi version
          } else {
            tagsVertices[tagId] += 1;
            lastTagId = tagId;
          }
        } else {
          tagsVertices[tagId] += 1;
          spaceVertices++;
          lastTagId = tagId;
          lastVertexId = vId;
        }
        iter->next();
      }

      prefix = NebulaKeyUtils::edgePrefix(part);
      ret = env_->kvstore_->prefix(spaceId, part, prefix, &iter);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        continue;
      }

      while (iter && iter->valid()) {
        auto key = iter->key();
        auto edgeType = NebulaKeyUtils::getEdgeType(spaceVidLen, key);
        if (edgeType < 0 || edgetypeEdges.find(edgeType) == edgetypeEdges.end()) {
          iter->next();
          continue;
        }

        auto source = NebulaKeyUtils::getSrcId(spaceVidLen, key).str();
        auto ranking = NebulaKeyUtils::getRank(spaceVidLen, key);
        auto destination = NebulaKeyUtils::getDstId(spaceVidLen, key).str();

        if (source == lastSrcVertexId && edgeType == lastEdgeType && ranking == lastRank &&
            destination == lastDstVertexId) {
          // Multi version
        } else {
          spaceEdges++;
          edgetypeEdges[edgeType] += 1;
          lastSrcVertexId = source;
          lastEdgeType = edgeType;
          lastRank = ranking;
          lastDstVertexId = destination;
        }
        iter->next();
      }
    }

    for (auto& e : tagsVertices) {
      if (e.first == 1) {
        ASSERT_EQ(51, e.second);
      } else if (e.first == 2) {
        ASSERT_EQ(30, e.second);
      } else {
        ASSERT_EQ(0, e.second);
      }
    }

    for (auto& edge : edgetypeEdges) {
      if (edge.first == 101) {
        // Do not contain reverse edge datas.
        ASSERT_EQ(167, edge.second);
      } else {
        ASSERT_EQ(0, edge.second);
      }
    }
    ASSERT_EQ(81, spaceVertices);
    ASSERT_EQ(167, spaceEdges);
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
