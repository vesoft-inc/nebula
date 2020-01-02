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

#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobManager.h"

DECLARE_int32(ws_storage_http_port);
using ResultCode = nebula::kvstore::ResultCode;

namespace nebula {
namespace meta {

class JobManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        LOG(INFO) << "enter" << __func__;
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/JobManager.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());
        TestUtils::createSomeHosts(kv_.get());
        TestUtils::assembleSpace(kv_.get(), 1, 1);
        pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
        pool_->start(1);
        jobMgr = JobManager::getInstance();
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
    JobManager* jobMgr{nullptr};
};

TEST_F(JobManagerTest, reserveJobId) {
    auto jobId = jobMgr->reserveJobId();
    ASSERT_TRUE(jobId.ok());
    ASSERT_EQ(jobId.value(), 1);
}

TEST_F(JobManagerTest, buildJobDescription) {
    std::string cmd("compact");
    std::string para("test");
    std::vector<std::string> paras{cmd, para};
    auto jd = jobMgr->buildJobDescription(paras);
    ASSERT_TRUE(jd.ok());
    ASSERT_EQ(jd.value().id_, 1);
    ASSERT_EQ(jd.value().cmd_, cmd);
    ASSERT_EQ(jd.value().paras_[0], para);
}

TEST_F(JobManagerTest, addJob) {
    std::string cmd("compact");
    std::string para("test");
    std::vector<std::string> paras{cmd, para};
    auto jd = jobMgr->buildJobDescription(paras);
    int32_t jobId = jobMgr->addJob(jd.value());
    ASSERT_EQ(jobId, 1);
}

TEST_F(JobManagerTest, loadJobDescription) {
    std::string cmd("compact");
    std::string para("test");
    std::vector<std::string> paras{cmd, para};
    auto jd1 = jobMgr->buildJobDescription(paras);
    jd1.value().setStatus(JobStatus::Status::RUNNING);
    jd1.value().setStatus(JobStatus::Status::FINISHED);
    int32_t jobId = jobMgr->addJob(jd1.value());

    // jobMgr->save(jd1.value().jobKey(), jd1.value().jobVal());

    auto optJd2 = jobMgr->loadJobDescription(jobId);
    ASSERT_TRUE(optJd2);
    ASSERT_EQ(jd1.value().id_, optJd2->id_);
    ASSERT_EQ(jd1.value().cmd_, optJd2->cmd_);
    ASSERT_EQ(jd1.value().paras_, optJd2->paras_);
    ASSERT_EQ(jd1.value().status_, optJd2->status_);
    ASSERT_EQ(jd1.value().startTime_, optJd2->startTime_);
    ASSERT_EQ(jd1.value().stopTime_, optJd2->stopTime_);
}


TEST(JobUtilTest, dummy) {
    ASSERT_TRUE(JobUtil::jobPrefix().length() + sizeof(size_t) !=
              JobUtil::currJobKey().length());
}

TEST_F(JobManagerTest, showJobs) {
    std::string cmd1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{cmd1, para1};
    auto jd1 = jobMgr->buildJobDescription(paras1);
    jd1.value().setStatus(JobStatus::Status::RUNNING);
    jd1.value().setStatus(JobStatus::Status::FINISHED);
    jobMgr->addJob(jd1.value());

    std::string cmd2("flush");
    std::string para2("nba");
    std::vector<std::string> paras2{cmd2, para2};
    auto jd2 = jobMgr->buildJobDescription(paras2);
    jd2.value().setStatus(JobStatus::Status::RUNNING);
    jd2.value().setStatus(JobStatus::Status::FAILED);
    jobMgr->addJob(jd2.value());

    auto statusOrShowResult = jobMgr->showJobs();
    LOG(INFO) << "after show jobs";
    ASSERT_TRUE(statusOrShowResult.ok());

    auto& tbl = statusOrShowResult.value();
    ASSERT_EQ(tbl[0], cmd1);
    ASSERT_EQ(tbl[1], para1);
    ASSERT_EQ(tbl[2], JobStatus::toString(JobStatus::Status::FINISHED));
    ASSERT_EQ(tbl[3], JobUtil::strTimeT(jd1.value().startTime_));
    ASSERT_EQ(tbl[4], JobUtil::strTimeT(jd1.value().stopTime_));
    ASSERT_EQ(tbl[5], cmd2);
    ASSERT_EQ(tbl[6], para2);
    ASSERT_EQ(tbl[7], JobStatus::toString(JobStatus::Status::FAILED));
    ASSERT_EQ(tbl[8], JobUtil::strTimeT(jd2.value().startTime_));
    ASSERT_EQ(tbl[9], JobUtil::strTimeT(jd2.value().stopTime_));
}

TEST_F(JobManagerTest, showJob) {
    std::string cmd("compact");
    std::string para("test");
    std::vector<std::string> paras{cmd, para};
    auto jd = jobMgr->buildJobDescription(paras);
    jd.value().setStatus(JobStatus::Status::RUNNING);
    jd.value().setStatus(JobStatus::Status::FINISHED);
    jobMgr->addJob(jd.value());

    int32_t iJob = jd.value().id_;
    int32_t task1 = 0;
    std::string dest1{"192.168.8.5"};

    TaskDescription td1(iJob, task1, dest1);
    td1.setStatus(JobStatus::Status::RUNNING);
    td1.setStatus(JobStatus::Status::FINISHED);
    jobMgr->save(td1.taskKey(), td1.taskVal());

    int32_t task2 = 1;
    std::string dest2{"192.168.8.6"};
    TaskDescription td2(iJob, task2, dest2);
    td2.setStatus(JobStatus::Status::RUNNING);
    td2.setStatus(JobStatus::Status::FAILED);
    jobMgr->save(td2.taskKey(), td2.taskVal());

    LOG(INFO) << "before jobMgr->showJob";
    auto statusOrResult = jobMgr->showJob(iJob);
    LOG(INFO) << "after jobMgr->showJob";
    ASSERT_TRUE(statusOrResult.ok());
    auto& result = statusOrResult.value();
    for (auto& row : result) {
        LOG(INFO) << row;
    }
    ASSERT_EQ(result[0], cmd);
    ASSERT_EQ(result[1], para);
    ASSERT_EQ(result[2], JobStatus::toString(JobStatus::Status::FINISHED));
    ASSERT_EQ(result[3], JobUtil::strTimeT(jd.value().startTime_));
    ASSERT_EQ(result[4], JobUtil::strTimeT(jd.value().stopTime_));
    ASSERT_EQ(result[5], folly::stringPrintf("%d-%d", iJob, task1));
    ASSERT_EQ(result[6], dest1);
    ASSERT_EQ(result[7], JobStatus::toString(JobStatus::Status::FINISHED));
    ASSERT_EQ(result[8], JobUtil::strTimeT(*td1.startTime_));
    ASSERT_EQ(result[9], JobUtil::strTimeT(*td1.stopTime_));
    ASSERT_EQ(result[10], folly::stringPrintf("%d-%d", iJob, task2));
    ASSERT_EQ(result[11], dest2);
    ASSERT_EQ(result[12], JobStatus::toString(JobStatus::Status::FAILED));
    ASSERT_EQ(result[13], JobUtil::strTimeT(*td2.startTime_));
    ASSERT_EQ(result[14], JobUtil::strTimeT(*td2.stopTime_));
}

// TEST_F(JobManagerTest, backupJob) {
// }

// TEST_F(JobManagerTest, recoverJob) {
// }

TEST(JobDescriptionTest, ctor) {
    std::string cmd1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{para1};
    JobDescription jd1(1, cmd1, paras1);
    jd1.setStatus(JobStatus::Status::RUNNING);
    jd1.setStatus(JobStatus::Status::FINISHED);
    LOG(INFO) << "jd1 ctored";
}

TEST(JobDescriptionTest, ctor2) {
    std::string cmd1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{para1};
    JobDescription jd1(1, cmd1, paras1);
    jd1.setStatus(JobStatus::Status::RUNNING);
    jd1.setStatus(JobStatus::Status::FINISHED);
    LOG(INFO) << "jd1 ctored";

    std::string strKey = jd1.jobKey();
    std::string strVal = jd1.jobVal();
    JobDescription jd2(strKey, strVal);
}

TEST(JobDescriptionTest, ctor3) {
    std::string cmd1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{para1};
    JobDescription jd1(1, cmd1, paras1);
    jd1.setStatus(JobStatus::Status::RUNNING);
    jd1.setStatus(JobStatus::Status::FINISHED);
    LOG(INFO) << "jd1 ctored";

    std::string strKey = jd1.jobKey();
    std::string strVal = jd1.jobVal();
    folly::StringPiece spKey(&strKey[0], strKey.length());
    folly::StringPiece spVal(&strVal[0], strVal.length());
    JobDescription jd2(spKey, spVal);
}

TEST(JobDescriptionTest, parseKey) {
    int32_t iJob = std::pow(2, 16);
    std::string cmd{"compact"};
    std::vector<std::string> paras{"test"};
    JobDescription jd(iJob, cmd, paras);
    auto sKey = jd.jobKey();
    ASSERT_EQ(iJob, jd.getJobId());
    ASSERT_EQ(cmd, jd.getCommand());

    folly::StringPiece spKey(&sKey[0], sKey.length());
    auto parsedKeyId = JobDescription::parseKey(spKey);
    ASSERT_EQ(iJob, parsedKeyId);
}

TEST(JobDescriptionTest, parseVal) {
    int32_t iJob = std::pow(2, 15);
    std::string cmd{"flush"};
    std::vector<std::string> paras{"nba"};
    JobDescription jd(iJob, cmd, paras);
    auto status = JobStatus::Status::FINISHED;
    jd.setStatus(JobStatus::Status::RUNNING);
    jd.setStatus(status);
    auto startTime = jd.startTime_;
    auto stopTime = jd.stopTime_;

    auto strVal = jd.jobVal();

    folly::StringPiece rawVal(&strVal[0], strVal.length());
    auto parsedVal = JobDescription::parseVal(rawVal);
    ASSERT_EQ(cmd, std::get<0>(parsedVal));
    ASSERT_EQ(paras, std::get<1>(parsedVal));
    ASSERT_EQ(status, std::get<2>(parsedVal));
    ASSERT_EQ(startTime, std::get<3>(parsedVal));
    ASSERT_EQ(stopTime, std::get<4>(parsedVal));
}

TEST(JobDescriptionTest, dump) {
    int32_t iJob = std::pow(2, 5);
    std::string cmd{"flush"};
    std::vector<std::string> paras{"nba"};
    JobDescription jd(iJob, cmd, paras);
    jd.setStatus(JobStatus::Status::RUNNING);
    auto status = JobStatus::Status::FINISHED;
    jd.setStatus(status);

    auto table = jd.dump();
    ASSERT_EQ(table[0], cmd);
    ASSERT_EQ(table[1], paras[0]);
    ASSERT_EQ(table[2], JobStatus::toString(status));
}

TEST(TaskDescriptionTest, ctor) {
    int32_t iJob = std::pow(2, 4);
    int32_t iTask = 0;
    std::string dest{"192.168.8.5"};
    TaskDescription td(iJob, iTask, dest);
    auto status = JobStatus::Status::RUNNING;

    ASSERT_EQ(iJob, td.iJob_);
    ASSERT_EQ(iTask, td.iTask_);
    ASSERT_EQ(dest, td.dest_);
    ASSERT_EQ(status, td.status_);
}

TEST(TaskDescriptionTest, parseKey) {
    int32_t iJob = std::pow(2, 5);
    int32_t iTask = 0;
    std::string dest{"192.168.8.5"};
    TaskDescription td(iJob, iTask, dest);

    std::string strKey = td.taskKey();
    folly::StringPiece rawKey(&strKey[0], strKey.length());
    auto tup = TaskDescription::parseKey(rawKey);
    ASSERT_EQ(iJob, std::get<0>(tup));
    ASSERT_EQ(iTask, std::get<1>(tup));
}

TEST(TaskDescriptionTest, parseVal) {
    int32_t iJob = std::pow(2, 5);
    int32_t iTask = 0;
    std::string dest{"192.168.8.5"};

    TaskDescription td(iJob, iTask, dest);
    td.setStatus(JobStatus::Status::RUNNING);
    auto status = JobStatus::Status::FINISHED;
    td.setStatus(status);

    std::string strVal = td.taskVal();
    folly::StringPiece rawVal(&strVal[0], strVal.length());
    auto parsedVal = TaskDescription::parseVal(rawVal);

    ASSERT_EQ(td.dest_, std::get<0>(parsedVal));
    ASSERT_EQ(td.status_, std::get<1>(parsedVal));
    ASSERT_EQ(td.startTime_, std::get<2>(parsedVal));
    ASSERT_EQ(td.stopTime_, std::get<3>(parsedVal));
}

TEST(TaskDescriptionTest, dump) {
    int32_t iJob = std::pow(2, 5);
    int32_t iTask = 0;
    std::string dest{"192.168.8.5"};

    TaskDescription td(iJob, iTask, dest);
    auto status = JobStatus::Status::FINISHED;
    td.setStatus(status);

    auto table = td.dump();

    ASSERT_EQ(table[0], std::to_string(iJob) + "-" + std::to_string(iTask));
    ASSERT_EQ(table[1], dest);
    ASSERT_EQ(table[2], JobStatus::toString(status));
}

TEST(TaskDescriptionTest, ctor2) {
    int32_t iJob = std::pow(2, 6);
    int32_t iTask = 0;
    std::string dest{"192.168.8.5"};

    TaskDescription td1(iJob, iTask, dest);
    ASSERT_EQ(td1.status_, JobStatus::Status::RUNNING);
    auto status = JobStatus::Status::FINISHED;
    td1.setStatus(status);

    std::string strKey = td1.taskKey();
    std::string strVal = td1.taskVal();

    folly::StringPiece rawKey(&strKey[0], strKey.length());
    folly::StringPiece rawVal(&strVal[0], strVal.length());
    TaskDescription td2(rawKey, rawVal);

    ASSERT_EQ(td1.iJob_, td2.iJob_);
    ASSERT_EQ(td1.iTask_, td2.iTask_);
    ASSERT_EQ(td1.dest_, td2.dest_);
    ASSERT_EQ(td1.status_, td2.status_);
    ASSERT_EQ(td1.startTime_, td2.startTime_);
    ASSERT_EQ(td1.stopTime_, td2.stopTime_);
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
