/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

class RebuildIndexTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        LOG(INFO) << "SetUp RebuildIndexTest TestCase";
        FLAGS_rebuild_index_locked_threshold = 1;
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/RebuildIndexTest.XXXXXX");
        cluster_ = std::make_unique<nebula::mock::MockCluster>();
        cluster_->initStorageKV(rootPath_->path());
        env_ = cluster_->storageEnv_.get();
        manager_ = AdminTaskManager::instance();
        manager_->init();
    }

    static void TearDownTestCase() {
        LOG(INFO) << "TearDown RebuildIndexTest TestCase";
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(parts);
    parameter.set_task_specfic_paras({"4", "5"});

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(3);
    request.set_task_id(13);
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Check the data count
    LOG(INFO) << "Check rebuild tag index...";
    auto vidSizeRet = RebuildIndexTest::env_->schemaMan_->getSpaceVidLen(1);
    auto vidSize = vidSizeRet.value();
    EXPECT_LT(0, vidSize);
    int dataNum = 0;
    for (auto part : parts) {
        auto prefix = NebulaKeyUtils::vertexPrefix(part);
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(parts);
    parameter.set_task_specfic_paras({"103", "104"});

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(6);
    request.set_task_id(16);
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
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

    auto deleteVertices = [&] () mutable {
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    parameter.set_task_specfic_paras({"4", "5"});

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(1);
    request.set_task_id(11);
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);

    writer->addTask(deleteVertices).get();

    auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
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

    auto appendVertices = [&] () mutable {
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    parameter.set_task_specfic_paras({"4", "5"});

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(2);
    request.set_task_id(12);
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);
    writer->addTask(appendVertices).get();

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys(true)) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    }

    RebuildIndexTest::env_->rebuildIndexGuard_->clear();
    writer->stop();
    sleep(1);
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(3);
    request.set_task_id(13);
    parameter.set_task_specfic_paras({"4", "5"});
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys()) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    }

    RebuildIndexTest::env_->rebuildIndexGuard_->clear();
    sleep(1);
}

TEST_F(RebuildIndexTest, RebuildEdgeIndexWithDelete) {
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    auto deleteEdges = [&] () mutable {
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    parameter.set_task_specfic_paras({"103", "104"});

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(4);
    request.set_task_id(14);
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);

    writer->addTask(deleteEdges).get();

    auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
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
    sleep(1);
}

TEST_F(RebuildIndexTest, RebuildEdgeIndexWithAppend) {
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    auto appendEdges = [&] () mutable {
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    parameter.set_task_specfic_paras({"103", "104"});

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(5);
    request.set_task_id(15);
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
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
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    parameter.set_task_specfic_paras({"103", "104"});

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(6);
    request.set_task_id(16);
    request.set_para(std::move(parameter));

    auto callback = [](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild edge index...";
    for (auto& key : mock::MockData::mockServeIndexKeys()) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
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

