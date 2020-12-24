/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2021.2022 License,
 * attached with Common Clause Condition 2023.2024, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/test/TestUtils.h"
#include "meta/test/MockAdminClient.h"
#include "kvstore/Common.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/GetStatisProcessor.h"

namespace nebula {
namespace meta {

using ::testing::_;
using ::testing::ByMove;
using ::testing::Return;
using ::testing::DefaultValue;
using ::testing::NiceMock;
using ::testing::SetArgPointee;

class GetStatisTest : public ::testing::Test {
protected:
    void SetUp() override {
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/GetStatisTest.XXXXXX");
        mock::MockCluster cluster;
        kv_ = cluster.initMetaKV(rootPath_->path());

        DefaultValue<folly::Future<Status>>::SetFactory([] {
            return folly::Future<Status>(Status::OK());
        });

        jobMgr = JobManager::getInstance();
        jobMgr->status_ = JobManager::Status::NOT_START;
        jobMgr->init(kv_.get());
    }

    void TearDown() override {
        jobMgr->shutDown();
        kv_.reset();
        rootPath_.reset();
    }

    std::unique_ptr<fs::TempDir> rootPath_{nullptr};
    std::unique_ptr<kvstore::KVStore> kv_{nullptr};
    JobManager* jobMgr{nullptr};
};


TEST_F(GetStatisTest, StatisJob) {
    ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
    TestUtils::assembleSpace(kv_.get(), 1, 1);
    GraphSpaceID spaceId = 1;
    std::vector<std::string> paras{"test_space"};
    JobDescription statisJob(12, cpp2::AdminCmd::STATS, paras);
    NiceMock<MockAdminClient> adminClient;
    jobMgr->adminClient_ = &adminClient;
    auto rc = jobMgr->save(statisJob.jobKey(), statisJob.jobVal());
    ASSERT_EQ(rc, nebula::kvstore::ResultCode::SUCCEEDED);

    {
        // Job is not executed, job status is QUEUE.
        // Statis data does not exist.
        auto job1 = JobDescription::loadJobDescription(statisJob.id_, kv_.get());
        ASSERT_TRUE(job1);
        ASSERT_EQ(statisJob.id_, job1.value().id_);
        ASSERT_EQ(cpp2::JobStatus::QUEUE, job1.value().status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.code);

        // Directly find statis data in kvstore, statis data does not exist.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_NE(kvstore::ResultCode::SUCCEEDED, ret);

        auto res = job1->setStatus(cpp2::JobStatus::RUNNING);
        ASSERT_TRUE(res);
        auto retsav = jobMgr->save(job1->jobKey(), job1->jobVal());
        ASSERT_EQ(retsav, nebula::kvstore::ResultCode::SUCCEEDED);
    }

    // Run statis job, job finished.
    // Insert running status statis data in prepare function of runJobInternal.
    // Update statis data to finished or failed status in finish function of runJobInternal.
    auto result = jobMgr->runJobInternal(statisJob);
    ASSERT_TRUE(result);
    // JobManager does not set the job finished status in RunJobInternal function.
    // But set statis data.
    statisJob.setStatus(cpp2::JobStatus::FINISHED);
    jobMgr->save(statisJob.jobKey(), statisJob.jobVal());

    {
        auto job2 = JobDescription::loadJobDescription(statisJob.id_, kv_.get());
        ASSERT_TRUE(job2);
        ASSERT_EQ(statisJob.id_, job2.value().id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, job2.value().status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

        auto statisItem = resp.statis;
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.status);
        ASSERT_EQ(0, statisItem.tag_vertices.size());
        ASSERT_EQ(0, statisItem.edges.size());
        ASSERT_EQ(0, statisItem.space_vertices);
        ASSERT_EQ(0, statisItem.space_edges);

        // Directly find statis data in kvstore, statis data exists.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);

        auto statisItem1 = MetaServiceUtils::parseStatisVal(val);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem1.status);
        ASSERT_EQ(0, statisItem1.tag_vertices.size());
        ASSERT_EQ(0, statisItem1.edges.size());
        ASSERT_EQ(0, statisItem1.space_vertices);
        ASSERT_EQ(0, statisItem1.space_edges);
    }

    // Execute new statis job in same space.
    std::vector<std::string> paras1{"test_space"};
    JobDescription statisJob2(13, cpp2::AdminCmd::STATS, paras1);
    auto rc2 = jobMgr->save(statisJob2.jobKey(), statisJob2.jobVal());
    ASSERT_EQ(rc2, nebula::kvstore::ResultCode::SUCCEEDED);
    {
        // Job is not executed, job status is QUEUE.
        // Statis data exists, but it is the result of the last statis job execution.
        auto job1 = JobDescription::loadJobDescription(statisJob2.id_, kv_.get());
        ASSERT_TRUE(job1);
        ASSERT_EQ(statisJob2.id_, job1.value().id_);
        ASSERT_EQ(cpp2::JobStatus::QUEUE, job1.value().status_);

        // Success,  but statis data is the result of the last statis job.
        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

        auto statisItem = resp.statis;
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.status);
        ASSERT_EQ(0, statisItem.tag_vertices.size());
        ASSERT_EQ(0, statisItem.edges.size());
        ASSERT_EQ(0, statisItem.space_vertices);
        ASSERT_EQ(0, statisItem.space_edges);

        // Directly find statis data in kvstore, statis data exists.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);

        auto statisItem1 = MetaServiceUtils::parseStatisVal(val);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem1.status);
        ASSERT_EQ(0, statisItem1.tag_vertices.size());
        ASSERT_EQ(0, statisItem1.edges.size());
        ASSERT_EQ(0, statisItem1.space_vertices);
        ASSERT_EQ(0, statisItem1.space_edges);

        auto res = job1->setStatus(cpp2::JobStatus::RUNNING);
        ASSERT_TRUE(res);
        auto retsav = jobMgr->save(job1->jobKey(), job1->jobVal());
        ASSERT_EQ(retsav, nebula::kvstore::ResultCode::SUCCEEDED);
    }

    // Remove statis data.
    {
        auto key = MetaServiceUtils::statisKey(spaceId);
        folly::Baton<true, std::atomic> baton;
        kv_->asyncRemove(kDefaultSpaceId, kDefaultPartId, key,
                         [&](nebula::kvstore::ResultCode code) {
                             if (code != kvstore::ResultCode::SUCCEEDED) {
                                 LOG(ERROR) << "kvstore asyncRemove failed: " << code;
                             }
                             baton.post();
                         });
        baton.wait();

        // Directly find statis data in kvstore, statis data does not exist.
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_NE(kvstore::ResultCode::SUCCEEDED, ret);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }

    // Run statis job.
    // Insert running status statis data in prepare function of runJobInternal.
    // Update statis data to finished or failed status in finish function of runJobInternal.
    auto result2 = jobMgr->runJobInternal(statisJob2);
    ASSERT_TRUE(result2);
    // JobManager does not set the job finished status in RunJobInternal function.
    // But set statis data.
    statisJob2.setStatus(cpp2::JobStatus::FINISHED);
    jobMgr->save(statisJob2.jobKey(), statisJob2.jobVal());

    {
        auto job2 = JobDescription::loadJobDescription(statisJob2.id_, kv_.get());
        ASSERT_TRUE(job2);
        ASSERT_EQ(statisJob2.id_, job2.value().id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, job2.value().status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

        auto statisItem = resp.statis;
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.status);
        ASSERT_EQ(0, statisItem.tag_vertices.size());
        ASSERT_EQ(0, statisItem.edges.size());
        ASSERT_EQ(0, statisItem.space_vertices);
        ASSERT_EQ(0, statisItem.space_edges);

        // Directly find statis data in kvstore, statis data exists.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);

        auto statisItem1 = MetaServiceUtils::parseStatisVal(val);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem1.status);
        ASSERT_EQ(0, statisItem1.tag_vertices.size());
        ASSERT_EQ(0, statisItem1.edges.size());
        ASSERT_EQ(0, statisItem1.space_vertices);
        ASSERT_EQ(0, statisItem1.space_edges);
    }
}

TEST_F(GetStatisTest, MockSingleMachineTest) {
    GraphSpaceID spaceId = 1;
    // Because only send to leader, need to mock leader distribution
    std::unordered_map<HostAddr, LeaderParts> leaders = {{HostAddr("0", 0), {{1, {1}}}}};
    ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
    TestUtils::assembleSpace(kv_.get(), 1, 1, 1, 1);
    for (const auto& entry : leaders) {
        auto now = time::WallClock::fastNowInMilliSec();
        auto ret = ActiveHostsMan::updateHostInfo(
            kv_.get(),
            entry.first,
            HostInfo(now, cpp2::HostRole::STORAGE, NEBULA_STRINGIFY(GIT_INFO_SHA)),
            &entry.second);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);
    }

    NiceMock<MockAdminClient> adminClient;
    jobMgr->adminClient_ = &adminClient;

    // add statis job1
    JobID jobId1 = 1;
    std::vector<std::string> paras{"test_space"};
    JobDescription job1(jobId1, cpp2::AdminCmd::STATS, paras);
    jobMgr->addJob(job1, &adminClient);

    auto genItem = [] (int64_t count) -> cpp2::StatisItem {
        cpp2::StatisItem item;
        item.tag_vertices = {{"t1", count}, {"t2", count}};
        item.edges = {{"e1", count}, {"e2", count}};
        item.space_vertices = 2 * count;
        item.space_edges = 2 * count;
        return item;
    };

    EXPECT_CALL(adminClient, addTask(_, _, _, _, _, _, _, _, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<8>(genItem(100)),
                        Return(ByMove(folly::Future<Status>(Status::OK())))))
        .WillOnce(DoAll(SetArgPointee<8>(genItem(200)),
                        Return(ByMove(folly::Future<Status>(Status::OK())))));

    // check job result
    {
        sleep(1);
        auto desc = JobDescription::loadJobDescription(job1.id_, kv_.get());
        ASSERT_TRUE(desc);
        ASSERT_EQ(job1.id_, desc.value().id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.value().status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

        auto statisItem = resp.statis;
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.status);
        ASSERT_EQ(2, statisItem.tag_vertices.size());
        int64_t tagCount = 0;
        for (const auto& entry : statisItem.tag_vertices) {
            tagCount += entry.second;
        }
        ASSERT_EQ(200, tagCount);
        ASSERT_EQ(2, statisItem.edges.size());
        int64_t edgeCount = 0;
        for (const auto& entry : statisItem.edges) {
            edgeCount += entry.second;
        }
        ASSERT_EQ(200, edgeCount);
        ASSERT_EQ(200, statisItem.space_vertices);
        ASSERT_EQ(200, statisItem.space_edges);
    }

    // add statis job2 of same space
    JobID jobId2 = 2;
    JobDescription job2(jobId2, cpp2::AdminCmd::STATS, paras);
    jobMgr->addJob(job2, &adminClient);

    // check job result
    {
        sleep(1);
        auto desc = JobDescription::loadJobDescription(job2.id_, kv_.get());
        ASSERT_TRUE(desc);
        ASSERT_EQ(job2.id_, desc.value().id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.value().status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

        auto statisItem = resp.statis;
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.status);
        ASSERT_EQ(2, statisItem.tag_vertices.size());
        int64_t tagCount = 0;
        for (const auto& entry : statisItem.tag_vertices) {
            tagCount += entry.second;
        }
        ASSERT_EQ(400, tagCount);
        ASSERT_EQ(2, statisItem.edges.size());
        int64_t edgeCount = 0;
        for (const auto& entry : statisItem.edges) {
            edgeCount += entry.second;
        }
        ASSERT_EQ(400, edgeCount);
        ASSERT_EQ(400, statisItem.space_vertices);
        ASSERT_EQ(400, statisItem.space_edges);
    }
}

TEST_F(GetStatisTest, MockMultiMachineTest) {
    GraphSpaceID spaceId = 1;
    // Because only send to leader, need to mock leader distribution
    std::unordered_map<HostAddr, LeaderParts> leaders = {
        {HostAddr("0", 0), {{1, {1, 2}}}},
        {HostAddr("1", 1), {{1, {3, 4}}}},
        {HostAddr("2", 2), {{1, {5, 6}}}},
    };
    ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
    TestUtils::assembleSpace(kv_.get(), 1, 6, 3, 3);
    for (const auto& entry : leaders) {
        auto now = time::WallClock::fastNowInMilliSec();
        auto ret = ActiveHostsMan::updateHostInfo(
            kv_.get(),
            entry.first,
            HostInfo(now, cpp2::HostRole::STORAGE, NEBULA_STRINGIFY(GIT_INFO_SHA)),
            &entry.second);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);
    }

    NiceMock<MockAdminClient> adminClient;
    jobMgr->adminClient_ = &adminClient;

    // add statis job
    JobID jobId = 1;
    std::vector<std::string> paras{"test_space"};
    JobDescription job(jobId, cpp2::AdminCmd::STATS, paras);
    jobMgr->addJob(job, &adminClient);

    auto genItem = [] (int64_t count) -> cpp2::StatisItem {
        cpp2::StatisItem item;
        item.tag_vertices = {{"t1", count}, {"t2", count}};
        item.edges = {{"e1", count}, {"e2", count}};
        item.space_vertices = 2 * count;
        item.space_edges = 2 * count;
        return item;
    };

    EXPECT_CALL(adminClient, addTask(_, _, _, _, _, _, _, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<8>(genItem(100)),
                        Return(ByMove(folly::Future<Status>(Status::OK())))))
        .WillOnce(DoAll(SetArgPointee<8>(genItem(200)),
                        Return(ByMove(folly::Future<Status>(Status::OK())))))
        .WillOnce(DoAll(SetArgPointee<8>(genItem(300)),
                        Return(ByMove(folly::Future<Status>(Status::OK())))));

    // check job result
    {
        sleep(1);
        auto desc = JobDescription::loadJobDescription(job.id_, kv_.get());
        ASSERT_TRUE(desc);
        ASSERT_EQ(job.id_, desc.value().id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.value().status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

        auto statisItem = resp.statis;
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.status);
        ASSERT_EQ(2, statisItem.tag_vertices.size());
        int64_t tagCount = 0;
        for (const auto& entry : statisItem.tag_vertices) {
            tagCount += entry.second;
        }
        ASSERT_EQ((100 + 200 + 300) * 2, tagCount);
        ASSERT_EQ(2, statisItem.edges.size());
        int64_t edgeCount = 0;
        for (const auto& entry : statisItem.edges) {
            edgeCount += entry.second;
        }
        ASSERT_EQ((100 + 200 + 300) * 2, edgeCount);
        ASSERT_EQ((100 + 200 + 300) * 2, statisItem.space_vertices);
        ASSERT_EQ((100 + 200 + 300) * 2, statisItem.space_edges);
    }
}


}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

