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

std::string toTempKey(int32_t space, int32_t jobId) {
    std::string key = MetaServiceUtils::statisKey(space);
    return key.append(reinterpret_cast<const char*>(&jobId), sizeof(int32_t));
}

void copyData(kvstore::KVStore* kv,
              int32_t space,
              int32_t part,
              const std::string& keySrc,
              const std::string& keyDst) {
    std::string val;
    kv->get(space, part, keySrc, &val);

    std::vector<kvstore::KV> data{std::make_pair(keyDst, val)};
    folly::Baton<true, std::atomic> b;
    kv->asyncMultiPut(space, part, std::move(data), [&](auto) { b.post(); });
    b.wait();
}

void genTempData(int32_t spaceId, int jobId, kvstore::KVStore* kv) {
    auto statisKey = MetaServiceUtils::statisKey(spaceId);
    auto tempKey = toTempKey(spaceId, jobId);
    copyData(kv, 0, 0, statisKey, tempKey);
}

struct JobCallBack {
    JobCallBack(JobManager* jobMgr, int job, int task, int n)
        : jobMgr_(jobMgr), jobId_(job), taskId_(task), n_(n) {}

    folly::Future<nebula::Status> operator()() {
        cpp2::ReportTaskReq req;
        req.set_code(nebula::cpp2::ErrorCode::SUCCEEDED);
        req.set_job_id(jobId_);
        req.set_task_id(taskId_);

        cpp2::StatisItem item;
        item.set_tag_vertices({{"t1", n_}, {"t2", n_}});
        item.set_edges({{"e1", n_}, {"e2", n_}});
        item.set_space_vertices(2 * n_);
        item.set_space_edges(2 * n_);
        req.set_statis(item);
        jobMgr_->reportTaskFinish(req);
        return folly::Future<Status>(Status::OK());
    }

    JobManager* jobMgr_{nullptr};
    int32_t jobId_{-1};
    int32_t taskId_{-1};
    int32_t n_{-1};
};

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
        jobMgr->status_ = JobManager::JbmgrStatus::NOT_START;
        jobMgr->init(kv_.get());
    }

    void TearDown() override {
        jobMgr->shutDown();
        kv_.reset();
        rootPath_.reset();
    }

    // using AllLeaders = std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>>;
    using FakeHost = std::pair<HostAddr, ActiveHostsMan::AllLeaders>;
    FakeHost fakeHost(std::string ip, int port, int space, std::vector<int> parts) {
        HostAddr host(ip, port);
        ActiveHostsMan::AllLeaders leaders;
        for (auto i = 0U; i != parts.size(); ++i) {
            leaders[space].emplace_back();
            leaders[space].back().set_part_id(parts[i]);
            leaders[space].back().set_term(9999);
        }
        return std::make_pair(host, leaders);
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
    ASSERT_EQ(rc, nebula::cpp2::ErrorCode::SUCCEEDED);

    {
        // Job is not executed, job status is QUEUE.
        // Statis data does not exist.
        auto job1Ret = JobDescription::loadJobDescription(statisJob.id_, kv_.get());
        ASSERT_TRUE(nebula::ok(job1Ret));
        auto job1 = nebula::value(job1Ret);
        ASSERT_EQ(statisJob.id_, job1.id_);
        ASSERT_EQ(cpp2::JobStatus::QUEUE, job1.status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // Directly find statis data in kvstore, statis data does not exist.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

        auto res = job1.setStatus(cpp2::JobStatus::RUNNING);
        ASSERT_TRUE(res);
        auto retsav = jobMgr->save(job1.jobKey(), job1.jobVal());
        ASSERT_EQ(retsav, nebula::cpp2::ErrorCode::SUCCEEDED);
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
    auto jobId = statisJob.getJobId();
    auto statisKey = MetaServiceUtils::statisKey(spaceId);
    auto tempKey = toTempKey(spaceId, jobId);

    copyData(kv_.get(), 0, 0, statisKey, tempKey);
    jobMgr->jobFinished(jobId, cpp2::JobStatus::FINISHED);
    {
        auto job2Ret = JobDescription::loadJobDescription(statisJob.id_, kv_.get());
        ASSERT_TRUE(nebula::ok(job2Ret));
        auto job2 = nebula::value(job2Ret);
        ASSERT_EQ(statisJob.id_, job2.id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, job2.status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (resp.get_code() != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(INFO) << "resp.code=" << apache::thrift::util::enumNameSafe(resp.get_code());
        }
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto statisItem = resp.get_statis();
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.get_status());
        ASSERT_EQ(0, statisItem.get_tag_vertices().size());
        ASSERT_EQ(0, statisItem.get_edges().size());
        ASSERT_EQ(0, statisItem.get_space_vertices());
        ASSERT_EQ(0, statisItem.get_space_edges());

        // Directly find statis data in kvstore, statis data exists.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

        auto statisItem1 = MetaServiceUtils::parseStatisVal(val);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem1.get_status());
        ASSERT_EQ(0, statisItem1.get_tag_vertices().size());
        ASSERT_EQ(0, statisItem1.get_edges().size());
        ASSERT_EQ(0, statisItem1.get_space_vertices());
        ASSERT_EQ(0, statisItem1.get_space_edges());
    }

    // Execute new statis job in same space.
    std::vector<std::string> paras1{"test_space"};
    JobDescription statisJob2(13, cpp2::AdminCmd::STATS, paras1);
    auto rc2 = jobMgr->save(statisJob2.jobKey(), statisJob2.jobVal());
    ASSERT_EQ(rc2, nebula::cpp2::ErrorCode::SUCCEEDED);
    {
        // Job is not executed, job status is QUEUE.
        // Statis data exists, but it is the result of the last statis job execution.
        auto job1Ret = JobDescription::loadJobDescription(statisJob2.id_, kv_.get());
        ASSERT_TRUE(nebula::ok(job1Ret));
        auto job1 = nebula::value(job1Ret);
        ASSERT_EQ(statisJob2.id_, job1.id_);
        ASSERT_EQ(cpp2::JobStatus::QUEUE, job1.status_);

        // Success,  but statis data is the result of the last statis job.
        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto statisItem = resp.get_statis();
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.get_status());
        ASSERT_EQ(0, statisItem.get_tag_vertices().size());
        ASSERT_EQ(0, statisItem.get_edges().size());
        ASSERT_EQ(0, statisItem.get_space_vertices());
        ASSERT_EQ(0, statisItem.get_space_edges());

        // Directly find statis data in kvstore, statis data exists.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

        auto statisItem1 = MetaServiceUtils::parseStatisVal(val);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem1.get_status());
        ASSERT_EQ(0, statisItem1.get_tag_vertices().size());
        ASSERT_EQ(0, statisItem1.get_edges().size());
        ASSERT_EQ(0, statisItem1.get_space_vertices());
        ASSERT_EQ(0, statisItem1.get_space_edges());

        auto res = job1.setStatus(cpp2::JobStatus::RUNNING);
        ASSERT_TRUE(res);
        auto retsav = jobMgr->save(job1.jobKey(), job1.jobVal());
        ASSERT_EQ(retsav, nebula::cpp2::ErrorCode::SUCCEEDED);
    }

    // Remove statis data.
    {
        auto key = MetaServiceUtils::statisKey(spaceId);
        folly::Baton<true, std::atomic> baton;
        auto retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
        kv_->asyncRemove(kDefaultSpaceId, kDefaultPartId, key,
                         [&](nebula::cpp2::ErrorCode code) {
                             if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                                 retCode = code;
                                 LOG(ERROR) << "kvstore asyncRemove failed: "
                                            << apache::thrift::util::enumNameSafe(code);
                             }
                             baton.post();
                         });
        baton.wait();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);

        // Directly find statis data in kvstore, statis data does not exist.
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // Run statis job.
    // Insert running status statis data in prepare function of runJobInternal.
    // Update statis data to finished or failed status in finish function of runJobInternal.
    auto result2 = jobMgr->runJobInternal(statisJob2);

    auto jobId2 = statisJob2.getJobId();
    auto statisKey2 = MetaServiceUtils::statisKey(spaceId);
    auto tempKey2 = toTempKey(spaceId, jobId2);

    copyData(kv_.get(), 0, 0, statisKey2, tempKey2);
    jobMgr->jobFinished(jobId2, cpp2::JobStatus::FINISHED);

    ASSERT_TRUE(result2);
    // JobManager does not set the job finished status in RunJobInternal function.
    // But set statis data.
    statisJob2.setStatus(cpp2::JobStatus::FINISHED);
    jobMgr->save(statisJob2.jobKey(), statisJob2.jobVal());

    {
        auto job2Ret = JobDescription::loadJobDescription(statisJob2.id_, kv_.get());
        ASSERT_TRUE(nebula::ok(job2Ret));
        auto job2 = nebula::value(job2Ret);
        ASSERT_EQ(statisJob2.id_, job2.id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, job2.status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto statisItem = resp.get_statis();
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.get_status());
        ASSERT_EQ(0, statisItem.get_tag_vertices().size());
        ASSERT_EQ(0, statisItem.get_edges().size());
        ASSERT_EQ(0, statisItem.get_space_vertices());
        ASSERT_EQ(0, statisItem.get_space_edges());

        // Directly find statis data in kvstore, statis data exists.
        auto key = MetaServiceUtils::statisKey(spaceId);
        std::string val;
        auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

        auto statisItem1 = MetaServiceUtils::parseStatisVal(val);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem1.get_status());
        ASSERT_EQ(0, statisItem1.get_tag_vertices().size());
        ASSERT_EQ(0, statisItem1.get_edges().size());
        ASSERT_EQ(0, statisItem1.get_space_vertices());
        ASSERT_EQ(0, statisItem1.get_space_edges());
    }
}

TEST_F(GetStatisTest, MockSingleMachineTest) {
    GraphSpaceID spaceId = 1;
    // // Because only send to leader, need to mock leader distribution
    std::map<HostAddr, ActiveHostsMan::AllLeaders> allStorage;
    auto storage = fakeHost("0", 0, 1, {1});
    allStorage[storage.first] = storage.second;

    ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
    TestUtils::assembleSpace(kv_.get(), 1, 1, 1, 1);
    for (const auto& entry : allStorage) {
        auto now = time::WallClock::fastNowInMilliSec();
        auto ret = ActiveHostsMan::updateHostInfo(
            kv_.get(),
            entry.first,
            HostInfo(now, cpp2::HostRole::STORAGE, gitInfoSha()),
            &entry.second);
        ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);
    }

    NiceMock<MockAdminClient> adminClient;
    jobMgr->adminClient_ = &adminClient;

    // add statis job1
    JobID jobId1 = 1;
    std::vector<std::string> paras{"test_space"};
    JobDescription job1(jobId1, cpp2::AdminCmd::STATS, paras);
    jobMgr->addJob(job1, &adminClient);

    JobCallBack cb1(jobMgr, jobId1, 0, 100);
    JobCallBack cb2(jobMgr, 2, 0, 200);

    EXPECT_CALL(adminClient, addTask(_, _, _, _, _, _, _, _, _))
        .Times(2)
        .WillOnce(testing::InvokeWithoutArgs(cb1))
        .WillOnce(testing::InvokeWithoutArgs(cb2));

    // check job result
    {
        sleep(1);
        auto descRet = JobDescription::loadJobDescription(job1.id_, kv_.get());
        ASSERT_TRUE(nebula::ok(descRet));
        auto desc = nebula::value(descRet);
        ASSERT_EQ(job1.id_, desc.id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto statisItem = resp.get_statis();
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.get_status());
        ASSERT_EQ(2, statisItem.get_tag_vertices().size());
        int64_t tagCount = 0;
        for (const auto& entry : statisItem.get_tag_vertices()) {
            tagCount += entry.second;
        }
        ASSERT_EQ(200, tagCount);
        ASSERT_EQ(2, statisItem.get_edges().size());
        int64_t edgeCount = 0;
        for (const auto& entry : statisItem.get_edges()) {
            edgeCount += entry.second;
        }
        ASSERT_EQ(200, edgeCount);
        ASSERT_EQ(200, statisItem.get_space_vertices());
        ASSERT_EQ(200, statisItem.get_space_edges());
    }

    // add statis job2 of same space
    JobID jobId2 = 2;
    JobDescription job2(jobId2, cpp2::AdminCmd::STATS, paras);
    jobMgr->addJob(job2, &adminClient);

    // check job result
    {
        sleep(1);
        auto descRet = JobDescription::loadJobDescription(job2.id_, kv_.get());
        ASSERT_TRUE(nebula::ok(descRet));
        auto desc = nebula::value(descRet);
        ASSERT_EQ(job2.id_, desc.id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto statisItem = resp.get_statis();
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.get_status());
        ASSERT_EQ(2, statisItem.get_tag_vertices().size());
        int64_t tagCount = 0;
        for (const auto& entry : statisItem.get_tag_vertices()) {
            tagCount += entry.second;
        }
        ASSERT_EQ(400, tagCount);
        ASSERT_EQ(2, statisItem.get_edges().size());
        int64_t edgeCount = 0;
        for (const auto& entry : statisItem.get_edges()) {
            edgeCount += entry.second;
        }
        ASSERT_EQ(400, edgeCount);
        ASSERT_EQ(400, statisItem.get_space_vertices());
        ASSERT_EQ(400, statisItem.get_space_edges());
    }
}

TEST_F(GetStatisTest, MockMultiMachineTest) {
    GraphSpaceID spaceId = 1;
    // Because only send to leader, need to mock leader distribution
    std::map<HostAddr, ActiveHostsMan::AllLeaders> allStorage;
    auto s1 = fakeHost("0", 0, 1, {1, 2});
    auto s2 = fakeHost("1", 1, 1, {3, 4});
    auto s3 = fakeHost("2", 2, 1, {5, 6});
    allStorage[s1.first] = s1.second;
    allStorage[s2.first] = s2.second;
    allStorage[s3.first] = s3.second;


    ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
    TestUtils::assembleSpace(kv_.get(), 1, 6, 3, 3);
    for (const auto& entry : allStorage) {
        auto now = time::WallClock::fastNowInMilliSec();
        auto ret = ActiveHostsMan::updateHostInfo(
            kv_.get(),
            entry.first,
            HostInfo(now, cpp2::HostRole::STORAGE, gitInfoSha()),
            &entry.second);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    }

    NiceMock<MockAdminClient> adminClient;
    jobMgr->adminClient_ = &adminClient;

    // add statis job
    JobID jobId = 1;
    std::vector<std::string> paras{"test_space"};
    JobDescription job(jobId, cpp2::AdminCmd::STATS, paras);
    jobMgr->addJob(job, &adminClient);

    JobCallBack cb1(jobMgr, jobId, 0, 100);
    JobCallBack cb2(jobMgr, jobId, 1, 200);
    JobCallBack cb3(jobMgr, jobId, 2, 300);

    EXPECT_CALL(adminClient, addTask(_, _, _, _, _, _, _, _, _))
        .Times(3)
        .WillOnce(testing::InvokeWithoutArgs(cb1))
        .WillOnce(testing::InvokeWithoutArgs(cb2))
        .WillOnce(testing::InvokeWithoutArgs(cb3));

    // check job result
    {
        sleep(1);
        auto descRet = JobDescription::loadJobDescription(job.id_, kv_.get());
        ASSERT_TRUE(nebula::ok(descRet));
        auto desc = nebula::value(descRet);
        ASSERT_EQ(job.id_, desc.id_);
        ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.status_);

        cpp2::GetStatisReq req;
        req.set_space_id(spaceId);
        auto* processor = GetStatisProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto statisItem = resp.get_statis();
        ASSERT_EQ(cpp2::JobStatus::FINISHED, statisItem.get_status());
        ASSERT_EQ(2, statisItem.get_tag_vertices().size());
        int64_t tagCount = 0;
        for (const auto& entry : statisItem.get_tag_vertices()) {
            tagCount += entry.second;
        }
        ASSERT_EQ((100 + 200 + 300) * 2, tagCount);
        ASSERT_EQ(2, statisItem.get_edges().size());
        int64_t edgeCount = 0;
        for (const auto& entry : statisItem.get_edges()) {
            edgeCount += entry.second;
        }
        ASSERT_EQ((100 + 200 + 300) * 2, edgeCount);
        ASSERT_EQ((100 + 200 + 300) * 2, statisItem.get_space_vertices());
        ASSERT_EQ((100 + 200 + 300) * 2, statisItem.get_space_edges());
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
