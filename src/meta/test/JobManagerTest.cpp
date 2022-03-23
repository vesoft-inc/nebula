/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "kvstore/Common.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/job/JobManager.h"
#include "meta/processors/job/TaskDescription.h"
#include "meta/test/MockAdminClient.h"
#include "meta/test/TestUtils.h"
#include "webservice/WebService.h"

DECLARE_int32(ws_storage_http_port);

namespace nebula {
namespace meta {

using ::testing::DefaultValue;
using ::testing::NiceMock;

class JobManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/JobManager.XXXXXX");
    mock::MockCluster cluster;
    kv_ = cluster.initMetaKV(rootPath_->path());
    GraphSpaceID spaceId = 1;
    int32_t partitionNum = 1;
    ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));

    TestUtils::assembleSpace(kv_.get(), spaceId, partitionNum);

    // Make sure the rebuild job could find the index name.
    std::vector<cpp2::ColumnDef> columns;
    TestUtils::mockTagIndex(kv_.get(), 1, "tag_name", 11, "tag_index_name", columns);
    TestUtils::mockEdgeIndex(kv_.get(), 1, "edge_name", 21, "edge_index_name", columns);

    adminClient_ = std::make_unique<NiceMock<MockAdminClient>>();
    DefaultValue<folly::Future<Status>>::SetFactory(
        [] { return folly::Future<Status>(Status::OK()); });
  }

  std::unique_ptr<JobManager, std::function<void(JobManager*)>> getJobManager() {
    std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr(
        new JobManager(), [](JobManager* p) {
          std::tuple<JobManager::JbOp, JobID, GraphSpaceID> opJobId;
          while (p->jobSize() != 0) {
            p->tryDequeue(opJobId);
          }
          delete p;
        });
    jobMgr->status_ = JobManager::JbmgrStatus::NOT_START;
    jobMgr->kvStore_ = kv_.get();
    jobMgr->init(kv_.get());
    return jobMgr;
  }

  void TearDown() override {
    kv_.reset();
    rootPath_.reset();
  }

  std::unique_ptr<fs::TempDir> rootPath_{nullptr};
  std::unique_ptr<kvstore::KVStore> kv_{nullptr};
  std::unique_ptr<AdminClient> adminClient_{nullptr};
};

TEST_F(JobManagerTest, AddJob) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  // For preventing job schedule in JobManager
  jobMgr->status_ = JobManager::JbmgrStatus::STOPPED;
  jobMgr->bgThread_.join();
  GraphSpaceID spaceId = 1;
  JobID jobId = 2;
  JobDescription jobDesc(spaceId, jobId, cpp2::JobType::COMPACT);
  auto rc = jobMgr->addJob(jobDesc, adminClient_.get());
  ASSERT_EQ(rc, nebula::cpp2::ErrorCode::SUCCEEDED);
}

TEST_F(JobManagerTest, AddRebuildTagIndexJob) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  // For preventing job schedule in JobManager
  jobMgr->status_ = JobManager::JbmgrStatus::STOPPED;
  jobMgr->bgThread_.join();
  std::vector<std::string> paras{"tag_index_name"};
  GraphSpaceID spaceId = 1;
  JobID jobId = 11;
  JobDescription jobDesc(spaceId, jobId, cpp2::JobType::REBUILD_TAG_INDEX, paras);
  auto rc = jobMgr->addJob(jobDesc, adminClient_.get());
  ASSERT_EQ(rc, nebula::cpp2::ErrorCode::SUCCEEDED);
  auto result = jobMgr->runJobInternal(jobDesc, JobManager::JbOp::ADD);
  ASSERT_TRUE(result);
}

TEST_F(JobManagerTest, AddRebuildEdgeIndexJob) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  // For preventing job schedule in JobManager
  jobMgr->status_ = JobManager::JbmgrStatus::STOPPED;
  jobMgr->bgThread_.join();
  std::vector<std::string> paras{"edge_index_name"};
  GraphSpaceID spaceId = 1;
  JobID jobId = 11;
  JobDescription jobDesc(spaceId, jobId, cpp2::JobType::REBUILD_EDGE_INDEX, paras);
  auto rc = jobMgr->addJob(jobDesc, adminClient_.get());
  ASSERT_EQ(rc, nebula::cpp2::ErrorCode::SUCCEEDED);
  auto result = jobMgr->runJobInternal(jobDesc, JobManager::JbOp::ADD);
  ASSERT_TRUE(result);
}

TEST_F(JobManagerTest, StatsJob) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  // For preventing job schedule in JobManager
  jobMgr->status_ = JobManager::JbmgrStatus::STOPPED;
  jobMgr->bgThread_.join();
  GraphSpaceID spaceId = 1;
  JobID jobId = 12;
  JobDescription jobDesc(spaceId, jobId, cpp2::JobType::STATS);
  auto rc = jobMgr->addJob(jobDesc, adminClient_.get());
  ASSERT_EQ(rc, nebula::cpp2::ErrorCode::SUCCEEDED);
  auto result = jobMgr->runJobInternal(jobDesc, JobManager::JbOp::ADD);
  ASSERT_TRUE(result);
  // Function runJobInternal does not set the finished status of the job
  jobDesc.setStatus(cpp2::JobStatus::FINISHED);
  auto jobKey = MetaKeyUtils::jobKey(jobDesc.getSpace(), jobDesc.getJobId());
  auto jobVal = MetaKeyUtils::jobVal(jobDesc.getJobType(),
                                     jobDesc.getParas(),
                                     jobDesc.getStatus(),
                                     jobDesc.getStartTime(),
                                     jobDesc.getStopTime());
  jobMgr->save(std::move(jobKey), std::move(jobVal));

  auto job1Ret =
      JobDescription::loadJobDescription(jobDesc.getSpace(), jobDesc.getJobId(), kv_.get());
  ASSERT_TRUE(nebula::ok(job1Ret));
  auto job = nebula::value(job1Ret);
  ASSERT_EQ(job.getJobId(), jobDesc.getJobId());
  ASSERT_EQ(cpp2::JobStatus::FINISHED, job.getStatus());
}

// Jobs are parallelized between spaces, and serialized by priority within spaces
TEST_F(JobManagerTest, JobPriority) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  // For preventing job schedule in JobManager
  jobMgr->status_ = JobManager::JbmgrStatus::STOPPED;
  jobMgr->bgThread_.join();
  ASSERT_EQ(0, jobMgr->jobSize());

  GraphSpaceID spaceId = 1;
  JobID jobId1 = 13;
  JobDescription jobDesc1(spaceId, jobId1, cpp2::JobType::COMPACT);
  auto rc1 = jobMgr->addJob(jobDesc1, adminClient_.get());
  ASSERT_EQ(rc1, nebula::cpp2::ErrorCode::SUCCEEDED);

  JobID jobId2 = 14;
  JobDescription jobDesc2(spaceId, jobId2, cpp2::JobType::LEADER_BALANCE);
  auto rc2 = jobMgr->addJob(jobDesc2, adminClient_.get());
  ASSERT_EQ(rc2, nebula::cpp2::ErrorCode::SUCCEEDED);

  GraphSpaceID spaceId2 = 2;
  JobID jobId3 = 15;
  JobDescription jobDesc3(spaceId2, jobId3, cpp2::JobType::STATS);
  auto rc3 = jobMgr->addJob(jobDesc3, adminClient_.get());
  ASSERT_EQ(rc3, nebula::cpp2::ErrorCode::SUCCEEDED);

  ASSERT_EQ(3, jobMgr->jobSize());

  std::tuple<JobManager::JbOp, JobID, GraphSpaceID> opJobId;
  auto result = jobMgr->tryDequeue(opJobId);
  ASSERT_TRUE(result);
  ASSERT_EQ(14, std::get<1>(opJobId));
  ASSERT_EQ(spaceId, std::get<2>(opJobId));
  // Suppose job starts executing
  jobMgr->spaceRunningJobs_.insert_or_assign(spaceId, true);

  ASSERT_EQ(2, jobMgr->jobSize());

  result = jobMgr->tryDequeue(opJobId);
  ASSERT_TRUE(result);
  ASSERT_EQ(15, std::get<1>(opJobId));
  ASSERT_EQ(spaceId2, std::get<2>(opJobId));
  // Suppose job starts executing
  jobMgr->spaceRunningJobs_.insert_or_assign(spaceId2, true);

  ASSERT_EQ(1, jobMgr->jobSize());

  result = jobMgr->tryDequeue(opJobId);
  // Because all spaces are running jobs
  ASSERT_FALSE(result);

  // Suppose the job execution is complete
  jobMgr->spaceRunningJobs_.insert_or_assign(spaceId, false);
  jobMgr->spaceRunningJobs_.insert_or_assign(spaceId2, false);
  ASSERT_EQ(1, jobMgr->jobSize());
  result = jobMgr->tryDequeue(opJobId);
  ASSERT_TRUE(result);
  ASSERT_EQ(13, std::get<1>(opJobId));
  ASSERT_EQ(spaceId, std::get<2>(opJobId));
  ASSERT_EQ(0, jobMgr->jobSize());
}

TEST_F(JobManagerTest, JobDeduplication) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  // For preventing job schedule in JobManager
  jobMgr->status_ = JobManager::JbmgrStatus::STOPPED;
  jobMgr->bgThread_.join();
  ASSERT_EQ(0, jobMgr->jobSize());

  GraphSpaceID spaceId = 1;
  JobID jobId1 = 15;
  JobDescription jobDesc1(spaceId, jobId1, cpp2::JobType::COMPACT);
  auto rc1 = jobMgr->addJob(jobDesc1, adminClient_.get());
  ASSERT_EQ(rc1, nebula::cpp2::ErrorCode::SUCCEEDED);

  JobID jobId2 = 16;
  JobDescription jobDesc2(spaceId, jobId2, cpp2::JobType::LEADER_BALANCE);
  auto rc2 = jobMgr->addJob(jobDesc2, adminClient_.get());
  ASSERT_EQ(rc2, nebula::cpp2::ErrorCode::SUCCEEDED);

  ASSERT_EQ(2, jobMgr->jobSize());

  JobID jobId3 = 17;
  JobDescription jobDesc3(spaceId, jobId3, cpp2::JobType::LEADER_BALANCE);
  JobID jId3 = 0;
  auto jobExist = jobMgr->checkJobExist(spaceId, jobDesc3.getJobType(), jobDesc3.getParas(), jId3);
  if (!jobExist) {
    auto rc3 = jobMgr->addJob(jobDesc3, adminClient_.get());
    ASSERT_EQ(rc3, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  JobID jobId4 = 18;
  JobDescription jobDesc4(spaceId, jobId4, cpp2::JobType::COMPACT);
  JobID jId4 = 0;
  jobExist = jobMgr->checkJobExist(spaceId, jobDesc4.getJobType(), jobDesc4.getParas(), jId4);
  if (!jobExist) {
    auto rc4 = jobMgr->addJob(jobDesc4, adminClient_.get());
    ASSERT_NE(rc4, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  ASSERT_EQ(2, jobMgr->jobSize());
  std::tuple<JobManager::JbOp, JobID, GraphSpaceID> opJobId;
  auto result = jobMgr->tryDequeue(opJobId);
  ASSERT_TRUE(result);
  ASSERT_EQ(16, std::get<1>(opJobId));
  ASSERT_EQ(1, jobMgr->jobSize());

  result = jobMgr->tryDequeue(opJobId);
  ASSERT_TRUE(result);
  ASSERT_EQ(15, std::get<1>(opJobId));
  ASSERT_EQ(0, jobMgr->jobSize());

  result = jobMgr->tryDequeue(opJobId);
  ASSERT_FALSE(result);
}

TEST_F(JobManagerTest, LoadJobDescription) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  GraphSpaceID spaceId = 1;
  JobID jobId1 = 1;
  JobDescription jobDesc1(spaceId, jobId1, cpp2::JobType::COMPACT);
  jobDesc1.setStatus(cpp2::JobStatus::RUNNING);
  jobDesc1.setStatus(cpp2::JobStatus::FINISHED);
  auto rc = jobMgr->addJob(jobDesc1, adminClient_.get());
  ASSERT_EQ(rc, nebula::cpp2::ErrorCode::SUCCEEDED);
  ASSERT_EQ(jobDesc1.getSpace(), 1);
  ASSERT_EQ(jobDesc1.getJobId(), 1);
  ASSERT_EQ(jobDesc1.getJobType(), cpp2::JobType::COMPACT);
  ASSERT_TRUE(jobDesc1.getParas().empty());

  auto optJd2Ret =
      JobDescription::loadJobDescription(jobDesc1.getSpace(), jobDesc1.getJobId(), kv_.get());
  ASSERT_TRUE(nebula::ok(optJd2Ret));
  auto optJd2 = nebula::value(optJd2Ret);
  ASSERT_EQ(jobDesc1.getJobId(), optJd2.getJobId());
  ASSERT_EQ(jobDesc1.getSpace(), optJd2.getSpace());
  ASSERT_EQ(jobDesc1.getJobType(), optJd2.getJobType());
  ASSERT_EQ(jobDesc1.getParas(), optJd2.getParas());
  ASSERT_EQ(jobDesc1.getStatus(), optJd2.getStatus());
  ASSERT_EQ(jobDesc1.getStartTime(), optJd2.getStartTime());
  ASSERT_EQ(jobDesc1.getStopTime(), optJd2.getStopTime());
}

TEST_F(JobManagerTest, ShowJobs) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  GraphSpaceID spaceId = 1;
  JobID jobId1 = 1;
  JobDescription jobDesc1(spaceId, jobId1, cpp2::JobType::COMPACT);
  jobDesc1.setStatus(cpp2::JobStatus::RUNNING);
  jobDesc1.setStatus(cpp2::JobStatus::FINISHED);
  jobMgr->addJob(jobDesc1, adminClient_.get());

  JobID jobId2 = 2;
  JobDescription jobDesc2(spaceId, jobId2, cpp2::JobType::FLUSH);
  jobDesc2.setStatus(cpp2::JobStatus::RUNNING);
  jobDesc2.setStatus(cpp2::JobStatus::FAILED);
  jobMgr->addJob(jobDesc2, adminClient_.get());

  auto statusOrShowResult = jobMgr->showJobs(spaceId);
  LOG(INFO) << "after show jobs";
  ASSERT_TRUE(nebula::ok(statusOrShowResult));

  auto& jobs = nebula::value(statusOrShowResult);
  ASSERT_EQ(jobs[0].get_space_id(), jobDesc2.getSpace());
  ASSERT_EQ(jobs[0].get_job_id(), jobDesc2.getJobId());
  ASSERT_EQ(jobs[0].get_type(), cpp2::JobType::FLUSH);
  ASSERT_EQ(jobs[0].get_status(), cpp2::JobStatus::FAILED);
  ASSERT_EQ(jobs[0].get_start_time(), jobDesc2.getStartTime());
  ASSERT_EQ(jobs[0].get_stop_time(), jobDesc2.getStopTime());
  ASSERT_EQ(jobs[0].get_paras(), jobDesc2.getParas());

  ASSERT_EQ(jobs[1].get_space_id(), jobDesc1.getSpace());
  ASSERT_EQ(jobs[1].get_job_id(), jobDesc1.getJobId());
  ASSERT_EQ(jobs[1].get_type(), cpp2::JobType::COMPACT);
  ASSERT_EQ(jobs[1].get_status(), cpp2::JobStatus::FINISHED);
  ASSERT_EQ(jobs[1].get_start_time(), jobDesc1.getStartTime());
  ASSERT_EQ(jobs[1].get_stop_time(), jobDesc1.getStopTime());
  ASSERT_EQ(jobs[1].get_paras(), jobDesc1.getParas());
}

TEST_F(JobManagerTest, ShowJobsFromMultiSpace) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  GraphSpaceID spaceId1 = 1;
  JobID jobId1 = 1;
  JobDescription jd1(spaceId1, jobId1, cpp2::JobType::COMPACT);
  jd1.setStatus(cpp2::JobStatus::RUNNING);
  jd1.setStatus(cpp2::JobStatus::FINISHED);
  jobMgr->addJob(jd1, adminClient_.get());

  GraphSpaceID spaceId2 = 2;
  JobID jobId2 = 2;
  JobDescription jd2(spaceId2, jobId2, cpp2::JobType::FLUSH);
  jd2.setStatus(cpp2::JobStatus::RUNNING);
  jd2.setStatus(cpp2::JobStatus::FAILED);
  jobMgr->addJob(jd2, adminClient_.get());

  auto statusOrShowResult = jobMgr->showJobs(spaceId2);
  LOG(INFO) << "after show jobs";
  ASSERT_TRUE(nebula::ok(statusOrShowResult));

  auto& jobs = nebula::value(statusOrShowResult);
  ASSERT_EQ(jobs.size(), 1);

  ASSERT_EQ(jobs[0].get_space_id(), jd2.getSpace());
  ASSERT_EQ(jobs[0].get_job_id(), jd2.getJobId());
  ASSERT_EQ(jobs[0].get_type(), cpp2::JobType::FLUSH);
  ASSERT_EQ(jobs[0].get_status(), cpp2::JobStatus::FAILED);
  ASSERT_EQ(jobs[0].get_start_time(), jd2.getStartTime());
  ASSERT_EQ(jobs[0].get_stop_time(), jd2.getStopTime());
  ASSERT_EQ(jobs[0].get_paras(), jd2.getParas());
}

HostAddr toHost(std::string strIp) {
  return HostAddr(strIp, 0);
}

TEST_F(JobManagerTest, ShowJob) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  GraphSpaceID spaceId = 1;
  JobID jobId1 = 1;
  JobDescription jd(spaceId, jobId1, cpp2::JobType::COMPACT);
  jd.setStatus(cpp2::JobStatus::RUNNING);
  jd.setStatus(cpp2::JobStatus::FINISHED);
  jobMgr->addJob(jd, adminClient_.get());

  int32_t jobId2 = jd.getJobId();
  int32_t task1 = 0;
  auto host1 = toHost("127.0.0.1");

  TaskDescription td1(spaceId, jobId2, task1, host1);
  td1.setStatus(cpp2::JobStatus::RUNNING);
  td1.setStatus(cpp2::JobStatus::FINISHED);
  auto taskKey1 = MetaKeyUtils::taskKey(td1.getSpace(), td1.getJobId(), td1.getTaskId());
  auto taskVal1 =
      MetaKeyUtils::taskVal(td1.getHost(), td1.getStatus(), td1.getStartTime(), td1.getStopTime());
  jobMgr->save(taskKey1, taskVal1);

  int32_t task2 = 1;
  auto host2 = toHost("127.0.0.1");
  TaskDescription td2(spaceId, jobId2, task2, host2);
  td2.setStatus(cpp2::JobStatus::RUNNING);
  td2.setStatus(cpp2::JobStatus::FAILED);
  auto taskKey2 = MetaKeyUtils::taskKey(td2.getSpace(), td2.getJobId(), td2.getTaskId());
  auto taskVal2 =
      MetaKeyUtils::taskVal(td2.getHost(), td2.getStatus(), td2.getStartTime(), td2.getStopTime());
  jobMgr->save(taskKey2, taskVal2);

  LOG(INFO) << "before jobMgr->showJob";
  auto showResult = jobMgr->showJob(spaceId, jobId1);
  LOG(INFO) << "after jobMgr->showJob";
  ASSERT_TRUE(nebula::ok(showResult));
  auto& jobs = nebula::value(showResult).first;
  auto& tasks = nebula::value(showResult).second;

  ASSERT_EQ(jobs.get_space_id(), spaceId);
  ASSERT_EQ(jobs.get_job_id(), jobId1);
  ASSERT_EQ(jobs.get_type(), cpp2::JobType::COMPACT);
  ASSERT_TRUE(jobs.get_paras().empty());
  ASSERT_EQ(jobs.get_status(), cpp2::JobStatus::FINISHED);
  ASSERT_EQ(jobs.get_start_time(), jd.getStartTime());
  ASSERT_EQ(jobs.get_stop_time(), jd.getStopTime());

  ASSERT_EQ(tasks[0].get_space_id(), spaceId);
  ASSERT_EQ(tasks[0].get_job_id(), jobId1);
  ASSERT_EQ(tasks[0].get_task_id(), task1);
  ASSERT_EQ(tasks[0].get_host().host, host1.host);
  ASSERT_EQ(tasks[0].get_status(), cpp2::JobStatus::FINISHED);
  ASSERT_EQ(tasks[0].get_start_time(), td1.getStartTime());
  ASSERT_EQ(tasks[0].get_stop_time(), td1.getStopTime());

  ASSERT_EQ(tasks[1].get_space_id(), spaceId);
  ASSERT_EQ(tasks[1].get_job_id(), jobId1);
  ASSERT_EQ(tasks[1].get_task_id(), task2);
  ASSERT_EQ(tasks[1].get_host().host, host2.host);
  ASSERT_EQ(tasks[1].get_status(), cpp2::JobStatus::FAILED);
  ASSERT_EQ(tasks[1].get_start_time(), td2.getStartTime());
  ASSERT_EQ(tasks[1].get_stop_time(), td2.getStopTime());
}

TEST_F(JobManagerTest, ShowJobInOtherSpace) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  GraphSpaceID spaceId1 = 1;
  JobID jobId1 = 1;
  JobDescription jd(spaceId1, jobId1, cpp2::JobType::COMPACT);
  jd.setStatus(cpp2::JobStatus::RUNNING);
  jd.setStatus(cpp2::JobStatus::FINISHED);
  jobMgr->addJob(jd, adminClient_.get());

  int32_t jobId2 = jd.getJobId();
  int32_t task1 = 0;
  auto host1 = toHost("127.0.0.1");

  TaskDescription td1(spaceId1, jobId2, task1, host1);
  td1.setStatus(cpp2::JobStatus::RUNNING);
  td1.setStatus(cpp2::JobStatus::FINISHED);
  auto taskKey1 = MetaKeyUtils::taskKey(td1.getSpace(), td1.getJobId(), td1.getTaskId());
  auto taskVal1 =
      MetaKeyUtils::taskVal(td1.getHost(), td1.getStatus(), td1.getStartTime(), td1.getStopTime());
  jobMgr->save(taskKey1, taskVal1);

  int32_t task2 = 1;
  auto host2 = toHost("127.0.0.1");
  TaskDescription td2(spaceId1, jobId2, task2, host2);
  td2.setStatus(cpp2::JobStatus::RUNNING);
  td2.setStatus(cpp2::JobStatus::FAILED);
  auto taskKey2 = MetaKeyUtils::taskKey(td2.getSpace(), td2.getJobId(), td2.getTaskId());
  auto taskVal2 =
      MetaKeyUtils::taskVal(td2.getHost(), td2.getStatus(), td2.getStartTime(), td2.getStopTime());
  jobMgr->save(taskKey2, taskVal2);

  LOG(INFO) << "before jobMgr->showJob";
  GraphSpaceID spaceId2 = 2;
  auto showResult = jobMgr->showJob(spaceId2, jobId1);
  LOG(INFO) << "after jobMgr->showJob";
  ASSERT_TRUE(!nebula::ok(showResult));
}

TEST_F(JobManagerTest, RecoverJob) {
  std::unique_ptr<JobManager, std::function<void(JobManager*)>> jobMgr = getJobManager();
  // set status to prevent running the job since AdminClient is a injector
  jobMgr->status_ = JobManager::JbmgrStatus::STOPPED;
  jobMgr->bgThread_.join();
  GraphSpaceID spaceId = 1;

  int32_t nJob = 3;
  for (auto jobId = 0; jobId < nJob; ++jobId) {
    JobDescription jd(spaceId, jobId, cpp2::JobType::FLUSH);
    auto jobKey = MetaKeyUtils::jobKey(jd.getSpace(), jd.getJobId());
    auto jobVal = MetaKeyUtils::jobVal(
        jd.getJobType(), jd.getParas(), jd.getStatus(), jd.getStartTime(), jd.getStopTime());
    jobMgr->save(jobKey, jobVal);
  }

  auto nJobRecovered = jobMgr->recoverJob(spaceId, nullptr);
  ASSERT_EQ(nebula::value(nJobRecovered), 1);
}

TEST(JobDescriptionTest, Ctor) {
  GraphSpaceID spaceId = 1;
  JobID jobId1 = 1;
  JobDescription jd1(spaceId, jobId1, cpp2::JobType::COMPACT);
  jd1.setStatus(cpp2::JobStatus::RUNNING);
  jd1.setStatus(cpp2::JobStatus::FINISHED);
  LOG(INFO) << "jd1 ctored";
}

TEST(JobDescriptionTest, Ctor2) {
  GraphSpaceID spaceId = 1;
  JobID jobId1 = 1;
  JobDescription jd1(spaceId, jobId1, cpp2::JobType::COMPACT);
  jd1.setStatus(cpp2::JobStatus::RUNNING);
  jd1.setStatus(cpp2::JobStatus::FINISHED);
  LOG(INFO) << "jd1 ctored";

  auto jobKey = MetaKeyUtils::jobKey(jd1.getSpace(), jd1.getJobId());
  auto jobVal = MetaKeyUtils::jobVal(
      jd1.getJobType(), jd1.getParas(), jd1.getStatus(), jd1.getStartTime(), jd1.getStopTime());
  auto optJobRet = JobDescription::makeJobDescription(jobKey, jobVal);
  ASSERT_TRUE(nebula::ok(optJobRet));
}

TEST(JobDescriptionTest, ParseKey) {
  GraphSpaceID spaceId = 1;
  int32_t jobId = std::pow(2, 16);
  JobDescription jd(spaceId, jobId, cpp2::JobType::COMPACT);
  ASSERT_EQ(jobId, jd.getJobId());
  ASSERT_EQ(cpp2::JobType::COMPACT, jd.getJobType());

  auto jobKey = MetaKeyUtils::jobKey(jd.getSpace(), jd.getJobId());
  auto parsedSpaceJobId = MetaKeyUtils::parseJobKey(jobKey);
  ASSERT_EQ(spaceId, parsedSpaceJobId.first);
  ASSERT_EQ(jobId, parsedSpaceJobId.second);
}

TEST(JobDescriptionTest, ParseVal) {
  GraphSpaceID spaceId = 1;
  int32_t jobId = std::pow(2, 15);
  JobDescription jd(spaceId, jobId, cpp2::JobType::FLUSH);
  auto status = cpp2::JobStatus::FINISHED;
  jd.setStatus(cpp2::JobStatus::RUNNING);
  jd.setStatus(status);
  auto startTime = jd.getStartTime();
  auto stopTime = jd.getStopTime();

  auto jobVal = MetaKeyUtils::jobVal(
      jd.getJobType(), jd.getParas(), jd.getStatus(), jd.getStartTime(), jd.getStopTime());
  auto parsedVal = MetaKeyUtils::parseJobVal(jobVal);
  ASSERT_EQ(cpp2::JobType::FLUSH, std::get<0>(parsedVal));
  auto paras = std::get<1>(parsedVal);
  ASSERT_TRUE(paras.empty());
  ASSERT_EQ(status, std::get<2>(parsedVal));
  ASSERT_EQ(startTime, std::get<3>(parsedVal));
  ASSERT_EQ(stopTime, std::get<4>(parsedVal));
}

TEST(TaskDescriptionTest, Ctor) {
  GraphSpaceID spaceId = 1;
  int32_t jobId = std::pow(2, 4);
  int32_t taskId = 2;
  auto dest = toHost("");
  TaskDescription td(spaceId, jobId, taskId, dest);
  auto status = cpp2::JobStatus::RUNNING;

  ASSERT_EQ(spaceId, td.getSpace());
  ASSERT_EQ(jobId, td.getJobId());
  ASSERT_EQ(taskId, td.getTaskId());
  ASSERT_EQ(dest.host, td.getHost().host);
  ASSERT_EQ(dest.port, td.getHost().port);
  ASSERT_EQ(status, td.getStatus());
}

TEST(TaskDescriptionTest, ParseKey) {
  GraphSpaceID spaceId = 1;
  int32_t jobId = std::pow(2, 5);
  int32_t taskId = 2;
  std::string dest{"127.0.0.1"};
  TaskDescription td(spaceId, jobId, taskId, toHost(dest));

  auto taskKey = MetaKeyUtils::taskKey(td.getSpace(), td.getJobId(), td.getTaskId());

  auto tup = MetaKeyUtils::parseTaskKey(taskKey);
  ASSERT_EQ(td.getSpace(), std::get<0>(tup));
  ASSERT_EQ(td.getJobId(), std::get<1>(tup));
  ASSERT_EQ(td.getTaskId(), std::get<2>(tup));
}

TEST(TaskDescriptionTest, ParseVal) {
  GraphSpaceID spaceId = 1;
  int32_t jobId = std::pow(2, 5);
  int32_t taskId = 3;
  std::string dest{"127.0.0.1"};

  TaskDescription td(spaceId, jobId, taskId, toHost(dest));
  td.setStatus(cpp2::JobStatus::RUNNING);
  auto status = cpp2::JobStatus::FINISHED;
  td.setStatus(status);

  auto strVal =
      MetaKeyUtils::taskVal(td.getHost(), td.getStatus(), td.getStartTime(), td.getStopTime());
  auto parsedVal = MetaKeyUtils::parseTaskVal(strVal);

  ASSERT_EQ(td.getHost(), std::get<0>(parsedVal));
  ASSERT_EQ(td.getStatus(), std::get<1>(parsedVal));
  ASSERT_EQ(td.getStartTime(), std::get<2>(parsedVal));
  ASSERT_EQ(td.getStopTime(), std::get<3>(parsedVal));
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
