/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/ActiveHostsMan.h"
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "kvstore/Common.h"
#include "webservice/WebService.h"

#include "meta/KVJobManager.h"

DECLARE_int32(ws_storage_http_port);
using ResultCode = nebula::kvstore::ResultCode;

namespace nebula {
namespace meta {

class JobFooTest : public ::testing::Test {
protected:
    void SetUp() override {
        LOG(INFO) << "enter" << __func__;
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/MetaHttpIngestHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());
        TestUtils::createSomeHosts(kv_.get());
        TestUtils::assembleSpace(kv_.get(), 1, 1);
        pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
        pool_->start(1);
        jobMgr = KVJobManager::getInstance();
        jobMgr->init(kv_.get(), pool_.get());
        LOG(INFO) << "exit" << __func__;
    }
    void TearDown() override {
        LOG(INFO) << "enter" << __func__;
        jobMgr->shutDown();
        kv_.reset();
        rootPath_.reset();
        pool_->stop();
        LOG(INFO) << "exit" << __func__;
    }

    std::unique_ptr<fs::TempDir> rootPath_{nullptr};
    std::unique_ptr<kvstore::KVStore> kv_{nullptr};
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_{nullptr};
    KVJobManager* jobMgr{nullptr};
};

/*
 * 0. helper function
 * 1. add job
 * 2. show jobs 
 * 3. show job x
 * 4. stop job 
 * 5. backup job
 * 6. recover job
*/

TEST_F(JobFooTest, InternalHelper) {
    KVJobManager* job_mgr = jobMgr;
    {
        auto len_val = job_mgr->decodeLenVal("e35000a0");
        ASSERT_EQ(len_val.first, 5);
        ASSERT_EQ(len_val.second, 35000);
    }
    {
        LOG(INFO) << "job_mgr->kJobKey=" << job_mgr->kJobKey;
        std::string key = job_mgr->kJobKey + "e35000a0";
        LOG(INFO) << "key" << job_mgr->kJobPrefix;
        auto job_task = job_mgr->decodeJobKey(key);
        ASSERT_EQ(job_task.first, 35000);
        ASSERT_EQ(job_task.second, 0);
    }
    {
        auto job_task = job_mgr->decodeJobKey("__job_mgr__e35000b16");
        ASSERT_EQ(job_task.first, 35000);
        ASSERT_EQ(job_task.second, 16);
    }
}

TEST_F(JobFooTest, AddJob) {
    // step 1 add job from nothing
    std::string jobType("test");

    int base = 1000;
    jobMgr->setSingleVal(jobMgr->kCurrId, std::to_string(base-1));
    for (int i = base; i != base + 10; ++i) {
        auto job = jobMgr->addJob("test_op", "test_para");
        ASSERT_EQ(ok(job), true);
        EXPECT_EQ(i, value(job));
    }
    sleep(2);
}

TEST_F(JobFooTest, ShowJobs) {
    std::string jobType("test");

    int job_num = 10;
    int base = 4000;
    jobMgr->setSingleVal(jobMgr->kCurrId, std::to_string(base-1));
    for (int i = base; i != base + job_num; ++i) {
        auto job = jobMgr->addJob("test_op", "test_para");
        EXPECT_EQ(i, value(job));
    }

    // check if all job/task recorded (nth job has m+1 records)
    auto r1 = jobMgr->showJobs();
    for (auto& jr : r1) {
        LOG(INFO) << jr;
    }
    ASSERT_EQ(job_num, r1.size());

    for (int i = 0; i != job_num; ++i) {
        EXPECT_TRUE(r1[i].find(std::to_string(base+i)) != std::string::npos);
    }

    auto job_valid = base + job_num / 2;
    LOG(INFO) << "job_valid=" << job_valid;
    auto r2 = jobMgr->showJob(job_valid);
    for (auto& jr : r2) {
        LOG(INFO) << jr;
    }
    EXPECT_EQ(r2.size(), 1);
    for (size_t i = 0; i != r2.size(); ++i) {
        EXPECT_TRUE(r2[i].find(std::to_string(job_valid)) != std::string::npos);
    }

    // show an invalid job (base + n + 1)
    auto r3 = jobMgr->showJob(base + job_num + 1);
    EXPECT_EQ(r3.size(), 0);
}

/*
 * 1. Stop an valid job
 * 2. Stop an invalid job
 */
TEST_F(JobFooTest, StopJob) {
    fs::TempDir rootPath("/tmp/JobManagerTest.XXXXXX", false);
    std::unique_ptr<kvstore::KVStore> store(TestUtils::initKV(rootPath.path()));
    std::string jobType("test");

    int job_num = 10;
    int base = 5000;
    jobMgr->setSingleVal(jobMgr->kCurrId, std::to_string(base-1));
    for (int i = base; i != base + job_num; ++i) {
        auto job = jobMgr->addJob("test_op", "test_para");
        EXPECT_EQ(i, value(job));
    }

    auto job_valid = base + job_num - 1;
    auto task_stopped = jobMgr->stopJob(job_valid);

    LOG(INFO) << "task_stopped " << task_stopped;
    std::vector<std::string> result = jobMgr->showJob(job_valid);
    for (std::string& row : result) {
        EXPECT_TRUE(row.find(std::to_string(job_valid)) != std::string::npos);
        EXPECT_TRUE(row.find("stopped") != std::string::npos);
    }
}

TEST_F(JobFooTest, BackupJob) {
    std::string jobType("test");
    int job_num = 10;
    int base = 6000;
    jobMgr->setSingleVal(jobMgr->kCurrId, std::to_string(base-1));
    for (int i = base; i != base + job_num; ++i) {
        auto job = jobMgr->addJob("test_op", "test_para");
        EXPECT_EQ(i, value(job));
    }

    auto last_job = base + job_num -1;
    auto r = jobMgr->backupJob(0, last_job);
    LOG(INFO) << "job backup num " << r;
    auto tShowJobs = jobMgr->showJobs();
    LOG(INFO) << "tShowJobs.size()=" << tShowJobs.size();
    EXPECT_EQ(tShowJobs.size(), 0);
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

