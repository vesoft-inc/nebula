/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/webservice/WebService.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/ActiveHostsMan.h"
#include "meta/test/TestUtils.h"
#include "kvstore/Common.h"
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
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/JobManager.XXXXXX");
        mock::MockCluster cluster;
        kv_ = cluster.initMetaKV(rootPath_->path());

        ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
        ASSERT_TRUE(TestUtils::assembleSpace(kv_.get(), 1, 1));

        // Make sure the rebuild job could find the index name.
        std::vector<cpp2::ColumnDef> columns;
        ASSERT_TRUE(TestUtils::mockTagIndex(kv_.get(), 1, "tag_name", 11,
                                            "tag_index_name", columns));
        ASSERT_TRUE(TestUtils::mockEdgeIndex(kv_.get(), 1, "edge_name", 21,
                                             "edge_index_name", columns));

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
    }

    std::unique_ptr<fs::TempDir> rootPath_{nullptr};
    std::unique_ptr<kvstore::KVStore> kv_{nullptr};
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_{nullptr};
    std::unique_ptr<AdminClient> adminClient_{nullptr};
    JobManager* jobMgr{nullptr};
};

TEST_F(JobManagerTest, addJob) {
    std::vector<std::string> paras{"test"};
    JobDescription job(1, cpp2::AdminCmd::COMPACT, paras);
    auto rc = jobMgr->addJob(job, adminClient_.get());
    ASSERT_EQ(rc, cpp2::ErrorCode::SUCCEEDED);
}


TEST_F(JobManagerTest, AddRebuildTagIndexJob) {
    std::vector<std::string> paras{"test_space" , "tag_index_name"};
    JobDescription job(11, cpp2::AdminCmd::REBUILD_TAG_INDEX, paras);
    auto rc = jobMgr->addJob(job, adminClient_.get());
    ASSERT_EQ(rc, cpp2::ErrorCode::SUCCEEDED);
    auto result = jobMgr->runJobInternal(job);
    ASSERT_TRUE(result);
}


TEST_F(JobManagerTest, AddRebuildEdgeIndexJob) {
    std::vector<std::string> paras{"test_space" , "edge_index_name"};
    JobDescription job(11, cpp2::AdminCmd::REBUILD_EDGE_INDEX, paras);
    auto rc = jobMgr->addJob(job, adminClient_.get());
    ASSERT_EQ(rc, cpp2::ErrorCode::SUCCEEDED);
    auto result = jobMgr->runJobInternal(job);
    ASSERT_TRUE(result);
}

TEST_F(JobManagerTest, StatisJob) {
    std::vector<std::string> paras{"test_space"};
    JobDescription job(12, cpp2::AdminCmd::STATS, paras);
    auto rc = jobMgr->addJob(job, adminClient_.get());
    ASSERT_EQ(rc, cpp2::ErrorCode::SUCCEEDED);
    auto result = jobMgr->runJobInternal(job);
    ASSERT_TRUE(result);
    // Function runJobInternal does not set the finished status of the job
    job.setStatus(cpp2::JobStatus::FINISHED);
    jobMgr->save(job.jobKey(), job.jobVal());

    auto job1 = JobDescription::loadJobDescription(job.id_, kv_.get());
    ASSERT_TRUE(job1);
    ASSERT_EQ(job.id_, job1.value().id_);
    ASSERT_EQ(cpp2::JobStatus::FINISHED, job1.value().status_);
}

TEST_F(JobManagerTest, JobPriority) {
    // For preventting job schedule in JobManager
    jobMgr->status_ = JobManager::Status::STOPPED;

    ASSERT_EQ(0, jobMgr->jobSize());

    std::vector<std::string> paras{"test"};
    JobDescription job1(13, cpp2::AdminCmd::COMPACT, paras);
    auto rc1 = jobMgr->addJob(job1, adminClient_.get());
    ASSERT_EQ(rc1, cpp2::ErrorCode::SUCCEEDED);

    std::vector<std::string> paras1{"test_space"};
    JobDescription job2(14, cpp2::AdminCmd::STATS, paras1);
    auto rc2 = jobMgr->addJob(job2, adminClient_.get());
    ASSERT_EQ(rc2, cpp2::ErrorCode::SUCCEEDED);

    ASSERT_EQ(2, jobMgr->jobSize());

    JobID jobId = 0;
    auto result = jobMgr->try_dequeue(jobId);
    ASSERT_TRUE(result);
    ASSERT_EQ(14, jobId);
    ASSERT_EQ(1, jobMgr->jobSize());

    result = jobMgr->try_dequeue(jobId);
    ASSERT_TRUE(result);
    ASSERT_EQ(13, jobId);
    ASSERT_EQ(0, jobMgr->jobSize());

    result = jobMgr->try_dequeue(jobId);
    ASSERT_FALSE(result);

    jobMgr->status_ = JobManager::Status::RUNNING;
}

TEST_F(JobManagerTest, JobDeduplication) {
    // For preventting job schedule in JobManager
    jobMgr->status_ = JobManager::Status::STOPPED;

    ASSERT_EQ(0, jobMgr->jobSize());

    std::vector<std::string> paras{"test"};
    JobDescription job1(15, cpp2::AdminCmd::COMPACT, paras);
    auto rc1 = jobMgr->addJob(job1, adminClient_.get());
    ASSERT_EQ(rc1, cpp2::ErrorCode::SUCCEEDED);

    std::vector<std::string> paras1{"test_space"};
    JobDescription job2(16, cpp2::AdminCmd::STATS, paras1);
    auto rc2 = jobMgr->addJob(job2, adminClient_.get());
    ASSERT_EQ(rc2, cpp2::ErrorCode::SUCCEEDED);

    ASSERT_EQ(2, jobMgr->jobSize());

    JobDescription job3(17, cpp2::AdminCmd::STATS, paras1);
    JobID jId3 = 0;
    auto jobExist = jobMgr->checkJobExist(job3.getCmd(), job3.getParas(), jId3);
    if (!jobExist) {
        auto rc3 = jobMgr->addJob(job3, adminClient_.get());
        ASSERT_EQ(rc3, cpp2::ErrorCode::SUCCEEDED);
    }

    JobDescription job4(18, cpp2::AdminCmd::COMPACT, paras);
    JobID jId4 = 0;
    jobExist = jobMgr->checkJobExist(job4.getCmd(), job4.getParas(), jId4);
    if (!jobExist) {
        auto rc4 = jobMgr->addJob(job4, adminClient_.get());
        ASSERT_NE(rc4, cpp2::ErrorCode::SUCCEEDED);
    }

    ASSERT_EQ(2, jobMgr->jobSize());
    JobID jobId = 0;
    auto result = jobMgr->try_dequeue(jobId);
    ASSERT_TRUE(result);
    ASSERT_EQ(16, jobId);
    ASSERT_EQ(1, jobMgr->jobSize());

    result = jobMgr->try_dequeue(jobId);
    ASSERT_TRUE(result);
    ASSERT_EQ(15, jobId);
    ASSERT_EQ(0, jobMgr->jobSize());

    result = jobMgr->try_dequeue(jobId);
    ASSERT_FALSE(result);
    jobMgr->status_ = JobManager::Status::RUNNING;
}

TEST_F(JobManagerTest, loadJobDescription) {
    std::vector<std::string> paras{"test_space"};
    JobDescription job1(1, cpp2::AdminCmd::COMPACT, paras);
    job1.setStatus(cpp2::JobStatus  ::RUNNING);
    job1.setStatus(cpp2::JobStatus::FINISHED);
    auto rc = jobMgr->addJob(job1, adminClient_.get());
    ASSERT_EQ(rc, cpp2::ErrorCode::SUCCEEDED);
    ASSERT_EQ(job1.id_, 1);
    ASSERT_EQ(job1.cmd_, cpp2::AdminCmd::COMPACT);
    ASSERT_EQ(job1.paras_[0], "test_space");

    auto optJd2 = JobDescription::loadJobDescription(job1.id_, kv_.get());
    ASSERT_TRUE(optJd2);
    ASSERT_EQ(job1.id_, optJd2.value().id_);
    LOG(INFO) << "job1.id_ = " << job1.id_;
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
    std::vector<std::string> paras1{"test_space"};
    JobDescription jd1(1, cpp2::AdminCmd::COMPACT, paras1);
    jd1.setStatus(cpp2::JobStatus::RUNNING);
    jd1.setStatus(cpp2::JobStatus::FINISHED);
    jobMgr->addJob(jd1, adminClient_.get());

    std::vector<std::string> paras2{"test_space"};
    JobDescription jd2(2, cpp2::AdminCmd::FLUSH, paras2);
    jd2.setStatus(cpp2::JobStatus::RUNNING);
    jd2.setStatus(cpp2::JobStatus::FAILED);
    jobMgr->addJob(jd2, adminClient_.get());

    auto statusOrShowResult = jobMgr->showJobs();
    LOG(INFO) << "after show jobs";
    ASSERT_TRUE(nebula::ok(statusOrShowResult));

    auto& jobs = nebula::value(statusOrShowResult);
    ASSERT_EQ(jobs[1].get_id(), jd1.id_);
    ASSERT_EQ(jobs[1].get_cmd(), cpp2::AdminCmd::COMPACT);
    ASSERT_EQ(jobs[1].get_paras()[0], "test_space");
    ASSERT_EQ(jobs[1].get_status(), cpp2::JobStatus::FINISHED);
    ASSERT_EQ(jobs[1].get_start_time(), jd1.startTime_);
    ASSERT_EQ(jobs[1].get_stop_time(), jd1.stopTime_);

    ASSERT_EQ(jobs[0].get_id(), jd2.id_);
    ASSERT_EQ(jobs[0].get_cmd(), cpp2::AdminCmd::FLUSH);
    ASSERT_EQ(jobs[0].get_paras()[0], "test_space");
    ASSERT_EQ(jobs[0].get_status(), cpp2::JobStatus::FAILED);
    ASSERT_EQ(jobs[0].get_start_time(), jd2.startTime_);
    ASSERT_EQ(jobs[0].get_stop_time(), jd2.stopTime_);
}

HostAddr toHost(std::string strIp) {
    return HostAddr(strIp, 0);
}

TEST_F(JobManagerTest, showJob) {
    std::vector<std::string> paras{"test_space"};

    JobDescription jd(1, cpp2::AdminCmd::COMPACT, paras);
    jd.setStatus(cpp2::JobStatus::RUNNING);
    jd.setStatus(cpp2::JobStatus::FINISHED);
    jobMgr->addJob(jd, adminClient_.get());

    int32_t iJob = jd.id_;
    int32_t task1 = 0;
    auto host1 = toHost("127.0.0.1");

    TaskDescription td1(iJob, task1, host1);
    td1.setStatus(cpp2::JobStatus::RUNNING);
    td1.setStatus(cpp2::JobStatus::FINISHED);
    jobMgr->save(td1.taskKey(), td1.taskVal());

    int32_t task2 = 1;
    auto host2 = toHost("127.0.0.1");
    TaskDescription td2(iJob, task2, host2);
    td2.setStatus(cpp2::JobStatus::RUNNING);
    td2.setStatus(cpp2::JobStatus::FAILED);
    jobMgr->save(td2.taskKey(), td2.taskVal());

    LOG(INFO) << "before jobMgr->showJob";
    auto showResult = jobMgr->showJob(iJob);
    LOG(INFO) << "after jobMgr->showJob";
    ASSERT_TRUE(nebula::ok(showResult));
    auto& jobs = nebula::value(showResult).first;
    auto& tasks = nebula::value(showResult).second;

    ASSERT_EQ(jobs.get_id(), iJob);
    ASSERT_EQ(jobs.get_cmd(), cpp2::AdminCmd::COMPACT);
    ASSERT_EQ(jobs.get_paras()[0], "test_space");
    ASSERT_EQ(jobs.get_status(), cpp2::JobStatus::FINISHED);
    ASSERT_EQ(jobs.get_start_time(), jd.startTime_);
    ASSERT_EQ(jobs.get_stop_time(), jd.stopTime_);

    ASSERT_EQ(tasks[0].get_task_id(), task1);
    ASSERT_EQ(tasks[0].get_job_id(), iJob);
    ASSERT_EQ(tasks[0].get_host().host, host1.host);
    ASSERT_EQ(tasks[0].get_status(), cpp2::JobStatus::FINISHED);
    ASSERT_EQ(tasks[0].get_start_time(), td1.startTime_);
    ASSERT_EQ(tasks[0].get_stop_time(), td1.stopTime_);

    ASSERT_EQ(tasks[1].get_task_id(), task2);
    ASSERT_EQ(tasks[1].get_job_id(), iJob);
    ASSERT_EQ(tasks[1].get_host().host, host2.host);
    ASSERT_EQ(tasks[1].get_status(), cpp2::JobStatus::FAILED);
    ASSERT_EQ(tasks[1].get_start_time(), td2.startTime_);
    ASSERT_EQ(tasks[1].get_stop_time(), td2.stopTime_);
}

TEST_F(JobManagerTest, recoverJob) {
    // set status to prevent running the job since AdminClient is a injector
    jobMgr->status_ = JobManager::Status::NOT_START;
    int32_t nJob = 3;
    for (auto i = 0; i != nJob; ++i) {
        JobDescription jd(i, cpp2::AdminCmd::FLUSH, {"test_space"});
        jobMgr->save(jd.jobKey(), jd.jobVal());
    }

    auto nJobRecovered = jobMgr->recoverJob();
    ASSERT_EQ(nebula::value(nJobRecovered), 1);
}

TEST(JobDescriptionTest, ctor) {
    std::vector<std::string> paras1{"test_space"};
    JobDescription jd1(1, cpp2::AdminCmd::COMPACT, paras1);
    jd1.setStatus(cpp2::JobStatus::RUNNING);
    jd1.setStatus(cpp2::JobStatus::FINISHED);
    LOG(INFO) << "jd1 ctored";
}

TEST(JobDescriptionTest, ctor2) {
    std::vector<std::string> paras1{"test_space"};
    JobDescription jd1(1, cpp2::AdminCmd::COMPACT, paras1);
    jd1.setStatus(cpp2::JobStatus::RUNNING);
    jd1.setStatus(cpp2::JobStatus::FINISHED);
    LOG(INFO) << "jd1 ctored";

    std::string strKey = jd1.jobKey();
    std::string strVal = jd1.jobVal();
    auto optJob = JobDescription::makeJobDescription(strKey, strVal);
    ASSERT_NE(optJob, folly::none);
}

TEST(JobDescriptionTest, ctor3) {
    std::vector<std::string> paras1{"test_space"};
    JobDescription jd1(1, cpp2::AdminCmd::COMPACT, paras1);
    jd1.setStatus(cpp2::JobStatus::RUNNING);
    jd1.setStatus(cpp2::JobStatus::FINISHED);
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
    std::vector<std::string> paras{"test_space"};
    JobDescription jd(iJob, cpp2::AdminCmd::COMPACT, paras);
    auto sKey = jd.jobKey();
    ASSERT_EQ(iJob, jd.getJobId());
    ASSERT_EQ(cpp2::AdminCmd::COMPACT, jd.getCmd());

    folly::StringPiece spKey(&sKey[0], sKey.length());
    auto parsedKeyId = JobDescription::parseKey(spKey);
    ASSERT_EQ(iJob, parsedKeyId);
}

TEST(JobDescriptionTest, parseVal) {
    int32_t iJob = std::pow(2, 15);
    std::vector<std::string> paras{"nba"};
    JobDescription jd(iJob, cpp2::AdminCmd::FLUSH, paras);
    auto status = cpp2::JobStatus::FINISHED;
    jd.setStatus(cpp2::JobStatus::RUNNING);
    jd.setStatus(status);
    auto startTime = jd.startTime_;
    auto stopTime = jd.stopTime_;

    auto strVal = jd.jobVal();
    folly::StringPiece rawVal(&strVal[0], strVal.length());
    auto parsedVal = JobDescription::parseVal(rawVal);
    ASSERT_EQ(cpp2::AdminCmd::FLUSH, std::get<0>(parsedVal));
    ASSERT_EQ(paras, std::get<1>(parsedVal));
    ASSERT_EQ(status, std::get<2>(parsedVal));
    ASSERT_EQ(startTime, std::get<3>(parsedVal));
    ASSERT_EQ(stopTime, std::get<4>(parsedVal));
}

TEST(TaskDescriptionTest, ctor) {
    int32_t iJob = std::pow(2, 4);
    int32_t iTask = 0;
    auto dest = toHost("");
    TaskDescription td(iJob, iTask, dest);
    auto status = cpp2::JobStatus::RUNNING;

    ASSERT_EQ(iJob, td.iJob_);
    ASSERT_EQ(iTask, td.iTask_);
    ASSERT_EQ(dest.host, td.dest_.host);
    ASSERT_EQ(dest.port, td.dest_.port);
    ASSERT_EQ(status, td.status_);
}

TEST(TaskDescriptionTest, parseKey) {
    int32_t iJob = std::pow(2, 5);
    int32_t iTask = 0;
    std::string dest{"127.0.0.1"};
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
    std::string dest{"127.0.0.1"};

    TaskDescription td(iJob, iTask, toHost(dest));
    td.setStatus(cpp2::JobStatus::RUNNING);
    auto status = cpp2::JobStatus::FINISHED;
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
    auto dest = toHost("127.0.0.1");

    TaskDescription td1(iJob, iTask, dest);
    ASSERT_EQ(td1.status_, cpp2::JobStatus::RUNNING);
    auto status = cpp2::JobStatus::FINISHED;
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
