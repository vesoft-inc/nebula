/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/Common.h"
#include "meta/processors/job/GetStatsProcessor.h"
#include "meta/processors/job/JobManager.h"
#include "meta/test/MockAdminClient.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

using ::testing::_;
using ::testing::ByMove;
using ::testing::DefaultValue;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::SetArgPointee;

std::string toTempKey(GraphSpaceID space, JobID jobId) {
  std::string key = MetaKeyUtils::statsKey(space);
  return key.append(reinterpret_cast<const char*>(&jobId), sizeof(JobID));
}

void copyData(kvstore::KVStore* kv,
              GraphSpaceID space,
              PartitionID part,
              const std::string& keySrc,
              const std::string& keyDst) {
  std::string val;
  kv->get(space, part, keySrc, &val);

  std::vector<kvstore::KV> data{std::make_pair(keyDst, val)};
  folly::Baton<true, std::atomic> b;
  kv->asyncMultiPut(space, part, std::move(data), [&](auto) { b.post(); });
  b.wait();
}

void genTempData(GraphSpaceID spaceId, JobID jobId, kvstore::KVStore* kv) {
  auto statsKey = MetaKeyUtils::statsKey(spaceId);
  auto tempKey = toTempKey(spaceId, jobId);
  copyData(kv, 0, 0, statsKey, tempKey);
}

struct JobCallBack {
  JobCallBack(JobManager* jobMgr, GraphSpaceID space, JobID job, TaskID task, int n)
      : jobMgr_(jobMgr), spaceId_(space), jobId_(job), taskId_(task), n_(n) {}

  folly::Future<nebula::Status> operator()() {
    cpp2::ReportTaskReq req;
    req.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    req.space_id_ref() = spaceId_;
    req.job_id_ref() = jobId_;
    req.task_id_ref() = taskId_;

    cpp2::StatsItem item;
    item.tag_vertices_ref() = {{"t1", n_}, {"t2", n_}};
    item.edges_ref() = {{"e1", n_}, {"e2", n_}};
    item.space_vertices_ref() = 2 * n_;
    item.space_edges_ref() = 2 * n_;
    req.stats_ref() = item;
    auto mutexIter = jobMgr_->muJobFinished_.find(spaceId_);
    if (mutexIter == jobMgr_->muJobFinished_.end()) {
      mutexIter =
          jobMgr_->muJobFinished_.emplace(spaceId_, std::make_unique<std::recursive_mutex>()).first;
    }
    mutexIter->second->unlock();
    jobMgr_->reportTaskFinish(req);
    return folly::Future<Status>(Status::OK());
  }

  JobManager* jobMgr_{nullptr};
  GraphSpaceID spaceId_{-1};
  JobID jobId_{-1};
  TaskID taskId_{-1};
  int32_t n_{-1};
};

class GetStatsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/GetStatsTest.XXXXXX");
    mock::MockCluster cluster;
    kv_ = cluster.initMetaKV(rootPath_->path());

    // write some random leader key into kv, make sure that job will find a target storage
    std::vector<nebula::kvstore::KV> data{
        std::make_pair(MetaKeyUtils::leaderKey(1, 1), MetaKeyUtils::leaderValV3(HostAddr(), 1))};
    folly::Baton<true, std::atomic> baton;
    kv_->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();

    DefaultValue<folly::Future<Status>>::SetFactory(
        [] { return folly::Future<Status>(Status::OK()); });
    DefaultValue<folly::Future<StatusOr<bool>>>::SetFactory(
        [] { return folly::Future<StatusOr<bool>>(true); });

    jobMgr = JobManager::getInstance();
    jobMgr->status_ = JobManager::JbmgrStatus::NOT_START;
    jobMgr->init(kv_.get());
  }

  void TearDown() override {
    jobMgr->shutDown();
    kv_.reset();
    rootPath_.reset();
  }

  // using AllLeaders = std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>>
  using FakeHost = std::pair<HostAddr, ActiveHostsMan::AllLeaders>;
  FakeHost fakeHost(std::string ip, int port, GraphSpaceID space, std::vector<int> parts) {
    HostAddr host(ip, port);
    ActiveHostsMan::AllLeaders leaders;
    for (auto i = 0U; i != parts.size(); ++i) {
      leaders[space].emplace_back();
      leaders[space].back().part_id_ref() = parts[i];
      leaders[space].back().term_ref() = 9999;
    }
    return std::make_pair(host, leaders);
  }

  std::unique_ptr<fs::TempDir> rootPath_{nullptr};
  std::unique_ptr<kvstore::KVStore> kv_{nullptr};
  JobManager* jobMgr{nullptr};
};

TEST_F(GetStatsTest, StatsJob) {
  ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
  GraphSpaceID spaceId = 1;
  int32_t partNum = 1;
  JobID jobId1 = 12;
  TestUtils::assembleSpace(kv_.get(), spaceId, partNum);

  JobDescription statsJob(spaceId, jobId1, cpp2::JobType::STATS);
  NiceMock<MockAdminClient> adminClient;
  jobMgr->adminClient_ = &adminClient;
  auto jobKey1 = MetaKeyUtils::jobKey(statsJob.getSpace(), statsJob.getJobId());
  auto jobVal1 = MetaKeyUtils::jobVal(statsJob.getJobType(),
                                      statsJob.getParas(),
                                      statsJob.getStatus(),
                                      statsJob.getStartTime(),
                                      statsJob.getStopTime(),
                                      statsJob.getErrorCode());
  auto rc = jobMgr->save(std::move(jobKey1), std::move(jobVal1));
  ASSERT_EQ(rc, nebula::cpp2::ErrorCode::SUCCEEDED);

  {
    // Job is not executed, job status is QUEUE.
    // Stats data does not exist.
    auto job1Ret =
        JobDescription::loadJobDescription(statsJob.getSpace(), statsJob.getJobId(), kv_.get());
    ASSERT_TRUE(nebula::ok(job1Ret));
    auto job11 = nebula::value(job1Ret);
    ASSERT_EQ(statsJob.getJobId(), job11.getJobId());
    ASSERT_EQ(cpp2::JobStatus::QUEUE, job11.getStatus());

    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // Directly find stats data in kvstore, stats data does not exist.
    auto key = MetaKeyUtils::statsKey(spaceId);
    std::string val;
    auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

    auto res = job11.setStatus(cpp2::JobStatus::RUNNING);
    ASSERT_TRUE(res);
    auto jobKey2 = MetaKeyUtils::jobKey(job11.getSpace(), job11.getJobId());
    auto jobVal2 = MetaKeyUtils::jobVal(job11.getJobType(),
                                        job11.getParas(),
                                        job11.getStatus(),
                                        job11.getStartTime(),
                                        job11.getStopTime(),
                                        job11.getErrorCode());
    auto retsav = jobMgr->save(std::move(jobKey2), std::move(jobVal2));
    ASSERT_EQ(retsav, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  // Run stats job, job finished.
  // Insert running status stats data in prepare function of runJobInternal.
  // Update stats data to finished or failed status in finish function of
  // runJobInternal.
  auto result = jobMgr->runJobInternal(statsJob, JobManager::JbOp::ADD).get();
  ASSERT_EQ(result, nebula::cpp2::ErrorCode::SUCCEEDED);
  // JobManager does not set the job finished status in RunJobInternal function.
  // But set stats data.
  statsJob.setStatus(cpp2::JobStatus::FINISHED);
  auto jobKey3 = MetaKeyUtils::jobKey(statsJob.getSpace(), statsJob.getJobId());
  auto jobVal3 = MetaKeyUtils::jobVal(statsJob.getJobType(),
                                      statsJob.getParas(),
                                      statsJob.getStatus(),
                                      statsJob.getStartTime(),
                                      statsJob.getStopTime(),
                                      statsJob.getErrorCode());
  jobMgr->save(std::move(jobKey3), std::move(jobVal3));

  auto statsKey = MetaKeyUtils::statsKey(spaceId);
  auto tempKey = toTempKey(spaceId, jobId1);

  copyData(kv_.get(), 0, 0, statsKey, tempKey);
  jobMgr->jobFinished(spaceId, statsJob.getJobId(), cpp2::JobStatus::FINISHED);
  jobMgr->resetSpaceRunning(spaceId);
  {
    auto job2Ret =
        JobDescription::loadJobDescription(statsJob.getSpace(), statsJob.getJobId(), kv_.get());
    ASSERT_TRUE(nebula::ok(job2Ret));
    auto job12 = nebula::value(job2Ret);
    ASSERT_EQ(statsJob.getJobId(), job12.getJobId());
    ASSERT_EQ(cpp2::JobStatus::FINISHED, job12.getStatus());

    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    if (resp.get_code() != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "resp.code=" << apache::thrift::util::enumNameSafe(resp.get_code());
    }
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    auto statsItem = resp.get_stats();
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem.get_status());
    ASSERT_EQ(0, statsItem.get_tag_vertices().size());
    ASSERT_EQ(0, statsItem.get_edges().size());
    ASSERT_EQ(0, statsItem.get_space_vertices());
    ASSERT_EQ(0, statsItem.get_space_edges());

    // Directly find stats data in kvstore, stats data exists.
    auto key = MetaKeyUtils::statsKey(spaceId);
    std::string val;
    auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

    auto statsItem1 = MetaKeyUtils::parseStatsVal(val);
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem1.get_status());
    ASSERT_EQ(0, statsItem1.get_tag_vertices().size());
    ASSERT_EQ(0, statsItem1.get_edges().size());
    ASSERT_EQ(0, statsItem1.get_space_vertices());
    ASSERT_EQ(0, statsItem1.get_space_edges());
  }

  // Execute new stats job in same space.
  JobID jobId2 = 13;
  JobDescription statsJob2(spaceId, jobId2, cpp2::JobType::STATS);
  auto jobKey4 = MetaKeyUtils::jobKey(statsJob2.getSpace(), statsJob2.getJobId());
  auto jobVal4 = MetaKeyUtils::jobVal(statsJob2.getJobType(),
                                      statsJob2.getParas(),
                                      statsJob2.getStatus(),
                                      statsJob2.getStartTime(),
                                      statsJob2.getStopTime(),
                                      statsJob2.getErrorCode());
  auto rc2 = jobMgr->save(std::move(jobKey4), std::move(jobVal4));
  ASSERT_EQ(rc2, nebula::cpp2::ErrorCode::SUCCEEDED);
  {
    // Job is not executed, job status is QUEUE.
    // Stats data exists, but it is the result of the last stats job
    // execution.
    auto job2Ret =
        JobDescription::loadJobDescription(statsJob2.getSpace(), statsJob2.getJobId(), kv_.get());
    ASSERT_TRUE(nebula::ok(job2Ret));
    auto job21 = nebula::value(job2Ret);
    ASSERT_EQ(statsJob2.getJobId(), job21.getJobId());
    ASSERT_EQ(cpp2::JobStatus::QUEUE, job21.getStatus());

    // Success,  but stats data is the result of the last stats job.
    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    auto statsItem = resp.get_stats();
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem.get_status());
    ASSERT_EQ(0, statsItem.get_tag_vertices().size());
    ASSERT_EQ(0, statsItem.get_edges().size());
    ASSERT_EQ(0, statsItem.get_space_vertices());
    ASSERT_EQ(0, statsItem.get_space_edges());

    // Directly find stats data in kvstore, stats data exists.
    auto key = MetaKeyUtils::statsKey(spaceId);
    std::string val;
    auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

    auto statsItem1 = MetaKeyUtils::parseStatsVal(val);
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem1.get_status());
    ASSERT_EQ(0, statsItem1.get_tag_vertices().size());
    ASSERT_EQ(0, statsItem1.get_edges().size());
    ASSERT_EQ(0, statsItem1.get_space_vertices());
    ASSERT_EQ(0, statsItem1.get_space_edges());

    auto res = job21.setStatus(cpp2::JobStatus::RUNNING);
    ASSERT_TRUE(res);
    auto jobKey5 = MetaKeyUtils::jobKey(job21.getSpace(), job21.getJobId());
    auto jobVal5 = MetaKeyUtils::jobVal(job21.getJobType(),
                                        job21.getParas(),
                                        job21.getStatus(),
                                        job21.getStartTime(),
                                        job21.getStopTime(),
                                        job21.getErrorCode());
    auto retsav = jobMgr->save(std::move(jobKey5), std::move(jobVal5));
    ASSERT_EQ(retsav, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  // Remove stats data.
  {
    auto key = MetaKeyUtils::statsKey(spaceId);
    folly::Baton<true, std::atomic> baton;
    auto retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
    kv_->asyncRemove(kDefaultSpaceId, kDefaultPartId, key, [&](nebula::cpp2::ErrorCode code) {
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        retCode = code;
        LOG(ERROR) << "kvstore asyncRemove failed: " << apache::thrift::util::enumNameSafe(code);
      }
      baton.post();
    });
    baton.wait();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);

    // Directly find stats data in kvstore, stats data does not exist.
    std::string val;
    auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }

  // Run stats job.
  // Insert running status stats data in prepare function of runJobInternal.
  // Update stats data to finished or failed status in finish function of
  // runJobInternal.
  auto result2 = jobMgr->runJobInternal(statsJob2, JobManager::JbOp::ADD).get();

  auto statsKey2 = MetaKeyUtils::statsKey(spaceId);
  auto tempKey2 = toTempKey(spaceId, jobId2);

  copyData(kv_.get(), 0, 0, statsKey2, tempKey2);
  jobMgr->jobFinished(spaceId, statsJob2.getJobId(), cpp2::JobStatus::FINISHED);
  jobMgr->resetSpaceRunning(spaceId);

  ASSERT_EQ(result2, nebula::cpp2::ErrorCode::SUCCEEDED);
  // JobManager does not set the job finished status in RunJobInternal function.
  // But set stats data.
  statsJob2.setStatus(cpp2::JobStatus::FINISHED);
  auto jobKey6 = MetaKeyUtils::jobKey(statsJob2.getSpace(), statsJob2.getJobId());
  auto jobVal6 = MetaKeyUtils::jobVal(statsJob2.getJobType(),
                                      statsJob2.getParas(),
                                      statsJob2.getStatus(),
                                      statsJob2.getStartTime(),
                                      statsJob2.getStopTime(),
                                      statsJob2.getErrorCode());
  jobMgr->save(std::move(jobKey6), std::move(jobVal6));

  {
    auto job2Ret =
        JobDescription::loadJobDescription(statsJob2.getSpace(), statsJob2.getJobId(), kv_.get());
    ASSERT_TRUE(nebula::ok(job2Ret));
    auto job22 = nebula::value(job2Ret);
    ASSERT_EQ(statsJob2.getJobId(), job22.getJobId());
    ASSERT_EQ(cpp2::JobStatus::FINISHED, job22.getStatus());

    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    auto statsItem = resp.get_stats();
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem.get_status());
    ASSERT_EQ(0, statsItem.get_tag_vertices().size());
    ASSERT_EQ(0, statsItem.get_edges().size());
    ASSERT_EQ(0, statsItem.get_space_vertices());
    ASSERT_EQ(0, statsItem.get_space_edges());

    // Directly find stats data in kvstore, stats data exists.
    auto key = MetaKeyUtils::statsKey(spaceId);
    std::string val;
    auto ret = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

    auto statsItem1 = MetaKeyUtils::parseStatsVal(val);
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem1.get_status());
    ASSERT_EQ(0, statsItem1.get_tag_vertices().size());
    ASSERT_EQ(0, statsItem1.get_edges().size());
    ASSERT_EQ(0, statsItem1.get_space_vertices());
    ASSERT_EQ(0, statsItem1.get_space_edges());
  }
}

TEST_F(GetStatsTest, MockSingleMachineTest) {
  GraphSpaceID spaceId = 1;
  int32_t partNum = 1;
  // Because only send to leader, need to mock leader distribution
  std::map<HostAddr, ActiveHostsMan::AllLeaders> allStorage;
  auto storage = fakeHost("0", 0, spaceId, {1});
  allStorage[storage.first] = storage.second;

  ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
  TestUtils::assembleSpace(kv_.get(), spaceId, partNum);
  std::vector<kvstore::KV> data;
  for (const auto& entry : allStorage) {
    auto now = time::WallClock::fastNowInMilliSec();
    auto ret = ActiveHostsMan::updateHostInfo(kv_.get(),
                                              entry.first,
                                              HostInfo(now, cpp2::HostRole::STORAGE, gitInfoSha()),
                                              data,
                                              &entry.second);
    ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  TestUtils::doPut(kv_.get(), data);
  NiceMock<MockAdminClient> adminClient;
  jobMgr->adminClient_ = &adminClient;

  // add stats job1
  JobID jobId1 = 1;
  JobDescription job1(spaceId, jobId1, cpp2::JobType::STATS);
  jobMgr->addJob(job1, &adminClient);

  JobCallBack cb1(jobMgr, spaceId, jobId1, 0, 100);
  JobCallBack cb2(jobMgr, spaceId, 2, 0, 200);

  EXPECT_CALL(adminClient, addTask(_, _, _, _, _, _, _))
      .Times(2)
      .WillOnce(testing::InvokeWithoutArgs(cb1))
      .WillOnce(testing::InvokeWithoutArgs(cb2));

  // check job result
  {
    sleep(1);
    auto descRet = JobDescription::loadJobDescription(job1.getSpace(), job1.getJobId(), kv_.get());
    ASSERT_TRUE(nebula::ok(descRet));
    auto desc = nebula::value(descRet);
    ASSERT_EQ(job1.getJobId(), desc.getJobId());
    ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.getStatus());

    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    auto statsItem = resp.get_stats();
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem.get_status());
    ASSERT_EQ(2, statsItem.get_tag_vertices().size());
    int64_t tagCount = 0;
    for (const auto& entry : statsItem.get_tag_vertices()) {
      tagCount += entry.second;
    }
    ASSERT_EQ(200, tagCount);
    ASSERT_EQ(2, statsItem.get_edges().size());
    int64_t edgeCount = 0;
    for (const auto& entry : statsItem.get_edges()) {
      edgeCount += entry.second;
    }
    ASSERT_EQ(200, edgeCount);
    ASSERT_EQ(200, statsItem.get_space_vertices());
    ASSERT_EQ(200, statsItem.get_space_edges());
  }

  // add stats job2 of same space
  JobID jobId2 = 2;
  JobDescription job2(spaceId, jobId2, cpp2::JobType::STATS);
  jobMgr->addJob(job2, &adminClient);

  // check job result
  {
    sleep(1);
    auto descRet = JobDescription::loadJobDescription(job2.getSpace(), job2.getJobId(), kv_.get());
    ASSERT_TRUE(nebula::ok(descRet));
    auto desc = nebula::value(descRet);
    ASSERT_EQ(job2.getJobId(), desc.getJobId());
    ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.getStatus());

    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    auto statsItem = resp.get_stats();
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem.get_status());
    ASSERT_EQ(2, statsItem.get_tag_vertices().size());
    int64_t tagCount = 0;
    for (const auto& entry : statsItem.get_tag_vertices()) {
      tagCount += entry.second;
    }
    ASSERT_EQ(400, tagCount);
    ASSERT_EQ(2, statsItem.get_edges().size());
    int64_t edgeCount = 0;
    for (const auto& entry : statsItem.get_edges()) {
      edgeCount += entry.second;
    }
    ASSERT_EQ(400, edgeCount);
    ASSERT_EQ(400, statsItem.get_space_vertices());
    ASSERT_EQ(400, statsItem.get_space_edges());
  }
}

TEST_F(GetStatsTest, MockMultiMachineTest) {
  GraphSpaceID spaceId = 1;
  // Because only send to leader, need to mock leader distribution
  std::map<HostAddr, ActiveHostsMan::AllLeaders> allStorage;
  auto s1 = fakeHost("0", 0, spaceId, {1, 2});
  auto s2 = fakeHost("1", 1, spaceId, {3, 4});
  auto s3 = fakeHost("2", 2, spaceId, {5, 6});
  allStorage[s1.first] = s1.second;
  allStorage[s2.first] = s2.second;
  allStorage[s3.first] = s3.second;

  ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
  TestUtils::assembleSpace(kv_.get(), spaceId, 6, 3, 3);
  std::vector<kvstore::KV> data;
  for (const auto& entry : allStorage) {
    auto now = time::WallClock::fastNowInMilliSec();
    auto ret = ActiveHostsMan::updateHostInfo(kv_.get(),
                                              entry.first,
                                              HostInfo(now, cpp2::HostRole::STORAGE, gitInfoSha()),
                                              data,
                                              &entry.second);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
  }

  TestUtils::doPut(kv_.get(), data);
  NiceMock<MockAdminClient> adminClient;
  jobMgr->adminClient_ = &adminClient;

  // add stats job
  JobID jobId = 1;
  JobDescription job(spaceId, jobId, cpp2::JobType::STATS);
  jobMgr->addJob(job, &adminClient);

  JobCallBack cb1(jobMgr, spaceId, jobId, 0, 100);
  JobCallBack cb2(jobMgr, spaceId, jobId, 1, 200);
  JobCallBack cb3(jobMgr, spaceId, jobId, 2, 300);

  EXPECT_CALL(adminClient, addTask(_, _, _, _, _, _, _))
      .Times(3)
      .WillOnce(testing::InvokeWithoutArgs(cb1))
      .WillOnce(testing::InvokeWithoutArgs(cb2))
      .WillOnce(testing::InvokeWithoutArgs(cb3));

  // check job result
  {
    sleep(1);
    auto descRet = JobDescription::loadJobDescription(job.getSpace(), job.getJobId(), kv_.get());
    ASSERT_TRUE(nebula::ok(descRet));
    auto desc = nebula::value(descRet);
    ASSERT_EQ(job.getJobId(), desc.getJobId());
    ASSERT_EQ(cpp2::JobStatus::FINISHED, desc.getStatus());

    cpp2::GetStatsReq req;
    req.space_id_ref() = spaceId;
    auto* processor = GetStatsProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    auto statsItem = resp.get_stats();
    ASSERT_EQ(cpp2::JobStatus::FINISHED, statsItem.get_status());
    ASSERT_EQ(2, statsItem.get_tag_vertices().size());
    int64_t tagCount = 0;
    for (const auto& entry : statsItem.get_tag_vertices()) {
      tagCount += entry.second;
    }
    ASSERT_EQ((100 + 200 + 300) * 2, tagCount);
    ASSERT_EQ(2, statsItem.get_edges().size());
    int64_t edgeCount = 0;
    for (const auto& entry : statsItem.get_edges()) {
      edgeCount += entry.second;
    }
    ASSERT_EQ((100 + 200 + 300) * 2, edgeCount);
    ASSERT_EQ((100 + 200 + 300) * 2, statsItem.get_space_vertices());
    ASSERT_EQ((100 + 200 + 300) * 2, statsItem.get_space_edges());
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
