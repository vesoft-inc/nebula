/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>

#include "base/Base.h"
#include "meta/ActiveHostsMan.h"
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "kvstore/Common.h"
#include "webservice/WebService.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#define private public
#pragma clang diagnostic pop

#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobManager.h"

DECLARE_int32(ws_storage_http_port);
using ResultCode = nebula::kvstore::ResultCode;

namespace nebula {
namespace meta {

using Status = nebula::meta::cpp2::JobStatus;

class JobManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        LOG(INFO) << "enter" << __func__;
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/JobManager.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());
        TestUtils::createSomeHosts(kv_.get());
        TestUtils::assembleSpace(kv_.get(), 1, 1);
        jobMgr = JobManager::getInstance();
        jobMgr->status_ = JobManager::Status::NOT_START;
        jobMgr->init(kv_.get());
        LOG(INFO) << "exit" << __func__;
    }
    void TearDown() override {
        LOG(INFO) << "enter" << __func__;
        jobMgr->shutDown();
        kv_.reset();
        rootPath_.reset();
        LOG(INFO) << "exit" << __func__;
    }

    std::unique_ptr<fs::TempDir> rootPath_{nullptr};
    std::unique_ptr<kvstore::KVStore> kv_{nullptr};
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_{nullptr};
    JobManager* jobMgr{nullptr};
};

TEST_F(JobManagerTest, addJob) {
    std::string cmd("compact");
    std::vector<std::string> paras{"test"};
    JobDescription job(1, cmd, paras);
    auto rc = jobMgr->addJob(job);
    ASSERT_EQ(rc, nebula::kvstore::ResultCode::SUCCEEDED);
}

TEST_F(JobManagerTest, loadJobDescription) {
    std::string cmd("compact");
    std::string para("test");
    std::vector<std::string> paras{para};
    JobDescription job1(1, cmd, paras);
    job1.setStatus(Status::RUNNING);
    job1.setStatus(Status::FINISHED);
    auto rc = jobMgr->addJob(job1);
    ASSERT_EQ(rc, ResultCode::SUCCEEDED);
    ASSERT_EQ(job1.id_, 1);
    ASSERT_EQ(job1.cmd_, cmd);
    ASSERT_EQ(job1.paras_[0], para);

    auto optJd2 = JobDescription::loadJobDescription(job1.id_, kv_.get());
    ASSERT_TRUE(optJd2);
    ASSERT_EQ(job1.id_, optJd2.value().id_);
    LOG(INFO) << "job1.id_=" << job1.id_;
    ASSERT_EQ(job1.cmd_, optJd2.value().cmd_);
    ASSERT_EQ(job1.paras_, optJd2.value().paras_);
    ASSERT_EQ(job1.status_, optJd2.value().status_);
    ASSERT_EQ(job1.startTime_, optJd2.value().startTime_);
    ASSERT_EQ(job1.stopTime_, optJd2.value().stopTime_);
}

TEST(JobUtilTest, dummy) {
    ASSERT_TRUE(JobUtil::jobPrefix().length() + sizeof(size_t) !=
              JobUtil::currJobKey().length());
}

TEST_F(JobManagerTest, showJobs) {
    std::string type1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{para1};
    JobDescription jd1(1, type1, paras1);
    jd1.setStatus(Status::RUNNING);
    jd1.setStatus(Status::FINISHED);
    jobMgr->addJob(jd1);

    std::string type2("flush");
    std::string para2("nba");
    std::vector<std::string> paras2{para2};
    JobDescription jd2(2, type2, paras2);
    jd2.setStatus(Status::RUNNING);
    jd2.setStatus(Status::FAILED);
    jobMgr->addJob(jd2);

    auto statusOrShowResult = jobMgr->showJobs();
    LOG(INFO) << "after show jobs";
    ASSERT_TRUE(nebula::ok(statusOrShowResult));

    auto& jobs = nebula::value(statusOrShowResult);
    ASSERT_EQ(jobs[1].get_id(), jd1.id_);
    ASSERT_EQ(jobs[1].get_cmd(), type1);
    ASSERT_EQ(jobs[1].get_paras()[0], para1);
    ASSERT_EQ(jobs[1].get_status(), Status::FINISHED);
    ASSERT_EQ(jobs[1].get_start_time(), jd1.startTime_);
    ASSERT_EQ(jobs[1].get_stop_time(), jd1.stopTime_);

    ASSERT_EQ(jobs[0].get_id(), jd2.id_);
    ASSERT_EQ(jobs[0].get_cmd(), type2);
    ASSERT_EQ(jobs[0].get_paras()[0], para2);
    ASSERT_EQ(jobs[0].get_status(), Status::FAILED);
    ASSERT_EQ(jobs[0].get_start_time(), jd2.startTime_);
    ASSERT_EQ(jobs[0].get_stop_time(), jd2.stopTime_);
}

nebula::cpp2::HostAddr toHost(std::string strIp) {
    nebula::cpp2::HostAddr host;
    int ip = 0;
    nebula::network::NetworkUtils::ipv4ToInt(strIp, ip);
    host.set_ip(ip);
    return host;
}

TEST_F(JobManagerTest, showJob) {
    std::string type("compact");
    std::string para("test");
    std::vector<std::string> paras{para};

    JobDescription jd(1, type, paras);
    jd.setStatus(Status::RUNNING);
    jd.setStatus(Status::FINISHED);
    jobMgr->addJob(jd);

    int32_t iJob = jd.id_;
    int32_t task1 = 0;
    auto host1 = toHost("192.168.8.5");

    TaskDescription td1(iJob, task1, host1);
    td1.setStatus(Status::RUNNING);
    td1.setStatus(Status::FINISHED);
    jobMgr->save(td1.taskKey(), td1.taskVal());

    int32_t task2 = 1;
    auto host2 = toHost("192.168.8.5");
    TaskDescription td2(iJob, task2, host2);
    td2.setStatus(Status::RUNNING);
    td2.setStatus(Status::FAILED);
    jobMgr->save(td2.taskKey(), td2.taskVal());

    LOG(INFO) << "before jobMgr->showJob";
    auto showResult = jobMgr->showJob(iJob);
    LOG(INFO) << "after jobMgr->showJob";
    ASSERT_TRUE(nebula::ok(showResult));
    auto& jobs = nebula::value(showResult).first;
    auto& tasks = nebula::value(showResult).second;

    ASSERT_EQ(jobs.get_id(), iJob);
    // ASSERT_EQ(jobs.get_cmdAndParas(), type + " " + para + " ");
    ASSERT_EQ(jobs.get_cmd(), type);
    ASSERT_EQ(jobs.get_paras()[0], para);
    ASSERT_EQ(jobs.get_status(), Status::FINISHED);
    ASSERT_EQ(jobs.get_start_time(), jd.startTime_);
    ASSERT_EQ(jobs.get_stop_time(), jd.stopTime_);

    ASSERT_EQ(tasks[0].get_task_id(), task1);
    ASSERT_EQ(tasks[0].get_job_id(), iJob);
    ASSERT_EQ(tasks[0].get_host(), host1);
    ASSERT_EQ(tasks[0].get_status(), Status::FINISHED);
    ASSERT_EQ(tasks[0].get_start_time(), td1.startTime_);
    ASSERT_EQ(tasks[0].get_stop_time(), td1.stopTime_);

    ASSERT_EQ(tasks[1].get_task_id(), task2);
    ASSERT_EQ(tasks[1].get_job_id(), iJob);
    ASSERT_EQ(tasks[1].get_host(), host2);
    ASSERT_EQ(tasks[1].get_status(), Status::FAILED);
    ASSERT_EQ(tasks[1].get_start_time(), td2.startTime_);
    ASSERT_EQ(tasks[1].get_stop_time(), td2.stopTime_);
}

TEST_F(JobManagerTest, recoverJob) {
    int32_t nJob = 3;
    for (auto i = 0; i != nJob; ++i) {
        JobDescription jd(i, "flush", {"test"});
        jobMgr->save(jd.jobKey(), jd.jobVal());
    }

    auto nJobRecovered = jobMgr->recoverJob();
    ASSERT_EQ(nebula::value(nJobRecovered), nJob);
}

TEST(JobDescriptionTest, ctor) {
    std::string type1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{para1};
    JobDescription jd1(1, type1, paras1);
    jd1.setStatus(Status::RUNNING);
    jd1.setStatus(Status::FINISHED);
    LOG(INFO) << "jd1 ctored";
}

TEST(JobDescriptionTest, ctor2) {
    std::string type1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{para1};
    JobDescription jd1(1, type1, paras1);
    jd1.setStatus(Status::RUNNING);
    jd1.setStatus(Status::FINISHED);
    LOG(INFO) << "jd1 ctored";

    std::string strKey = jd1.jobKey();
    std::string strVal = jd1.jobVal();
    auto optJob = JobDescription::makeJobDescription(strKey, strVal);
    ASSERT_NE(optJob, folly::none);
}

TEST(JobDescriptionTest, ctor3) {
    std::string type1("compact");
    std::string para1("test");
    std::vector<std::string> paras1{para1};
    JobDescription jd1(1, type1, paras1);
    jd1.setStatus(Status::RUNNING);
    jd1.setStatus(Status::FINISHED);
    LOG(INFO) << "jd1 ctored";

    std::string strKey = jd1.jobKey();
    std::string strVal = jd1.jobVal();
    folly::StringPiece spKey(&strKey[0], strKey.length());
    folly::StringPiece spVal(&strVal[0], strVal.length());
    auto optJob = JobDescription::makeJobDescription(spKey, spVal);
    ASSERT_NE(optJob, folly::none);
}

TEST(JobDescriptionTest, parseKey) {
    int32_t iJob = std::pow(2, 16);
    std::string type{"compact"};
    std::vector<std::string> paras{"test"};
    JobDescription jd(iJob, type, paras);
    auto sKey = jd.jobKey();
    ASSERT_EQ(iJob, jd.getJobId());
    ASSERT_EQ(type, jd.getCmd());

    folly::StringPiece spKey(&sKey[0], sKey.length());
    auto parsedKeyId = JobDescription::parseKey(spKey);
    ASSERT_EQ(iJob, parsedKeyId);
}

TEST(JobDescriptionTest, parseVal) {
    int32_t iJob = std::pow(2, 15);
    std::string type{"flush"};
    std::vector<std::string> paras{"nba"};
    JobDescription jd(iJob, type, paras);
    auto status = Status::FINISHED;
    jd.setStatus(Status::RUNNING);
    jd.setStatus(status);
    auto startTime = jd.startTime_;
    auto stopTime = jd.stopTime_;

    auto strVal = jd.jobVal();

    folly::StringPiece rawVal(&strVal[0], strVal.length());
    auto parsedVal = JobDescription::parseVal(rawVal);
    ASSERT_EQ(type, std::get<0>(parsedVal));
    ASSERT_EQ(paras, std::get<1>(parsedVal));
    ASSERT_EQ(status, std::get<2>(parsedVal));
    ASSERT_EQ(startTime, std::get<3>(parsedVal));
    ASSERT_EQ(stopTime, std::get<4>(parsedVal));
}

TEST(TaskDescriptionTest, ctor) {
    int32_t iJob = std::pow(2, 4);
    int32_t iTask = 0;
    auto dest = toHost("192.168.8.5");
    TaskDescription td(iJob, iTask, dest);
    auto status = Status::RUNNING;

    ASSERT_EQ(iJob, td.iJob_);
    ASSERT_EQ(iTask, td.iTask_);
    ASSERT_EQ(dest, td.dest_);
    ASSERT_EQ(status, td.status_);
}

TEST(TaskDescriptionTest, parseKey) {
    int32_t iJob = std::pow(2, 5);
    int32_t iTask = 0;
    std::string dest{"192.168.8.5"};
    TaskDescription td(iJob, iTask, toHost(dest));

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

    TaskDescription td(iJob, iTask, toHost(dest));
    td.setStatus(Status::RUNNING);
    auto status = Status::FINISHED;
    td.setStatus(status);

    std::string strVal = td.taskVal();
    folly::StringPiece rawVal(&strVal[0], strVal.length());
    auto parsedVal = TaskDescription::parseVal(rawVal);

    ASSERT_EQ(td.dest_, std::get<0>(parsedVal));
    ASSERT_EQ(td.status_, std::get<1>(parsedVal));
    ASSERT_EQ(td.startTime_, std::get<2>(parsedVal));
    ASSERT_EQ(td.stopTime_, std::get<3>(parsedVal));
}

TEST(TaskDescriptionTest, ctor2) {
    int32_t iJob = std::pow(2, 6);
    int32_t iTask = 0;
    auto dest = toHost("192.168.8.5");

    TaskDescription td1(iJob, iTask, dest);
    ASSERT_EQ(td1.status_, Status::RUNNING);
    auto status = Status::FINISHED;
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
