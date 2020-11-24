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
#include "kvstore/Common.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/GetStatisProcessor.h"

namespace nebula {
namespace meta {

class GetStatisTest : public ::testing::Test {
protected:
    void SetUp() override {
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/GetStatisTest.XXXXXX");
        mock::MockCluster cluster;
        kv_ = cluster.initMetaKV(rootPath_->path());

        ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
        ASSERT_TRUE(TestUtils::assembleSpace(kv_.get(), 1, 1));

        std::vector<Status> sts(14, Status::OK());
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        adminClient_ = std::make_unique<AdminClient>(std::move(injector));

        jobMgr = JobManager::getInstance();
        jobMgr->status_ = JobManager::Status::NOT_START;
        jobMgr->init(kv_.get());
    }

    void TearDown() override {
        jobMgr->shutDown();
        kv_.reset();
        rootPath_.reset();
        adminClient_.reset();
    }

    std::unique_ptr<fs::TempDir> rootPath_{nullptr};
    std::unique_ptr<kvstore::KVStore> kv_{nullptr};
    std::unique_ptr<AdminClient> adminClient_{nullptr};
    JobManager* jobMgr{nullptr};
};


TEST_F(GetStatisTest, StatisJob) {
    GraphSpaceID  spaceId = 1;
    std::vector<std::string> paras{"test_space"};
    JobDescription statisJob(12, cpp2::AdminCmd::STATS, paras);
    jobMgr->adminClient_ = adminClient_.get();
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

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

