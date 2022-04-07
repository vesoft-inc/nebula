/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/JobManager.h"

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include <boost/stacktrace.hpp>

#include "common/http/HttpClient.h"
#include "common/stats/StatsManager.h"
#include "common/time/WallClock.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/BalancePlan.h"
#include "meta/processors/job/JobStatus.h"
#include "meta/processors/job/TaskDescription.h"
#include "webservice/Common.h"

DEFINE_int32(job_check_intervals, 5000, "job intervals in us");
DEFINE_double(job_expired_secs, 7 * 24 * 60 * 60, "job expired intervals in sec");

using nebula::kvstore::KVIterator;

namespace nebula {
namespace meta {
stats::CounterId kNumRunningJobs;

JobManager* JobManager::getInstance() {
  static JobManager inst;
  return &inst;
}

bool JobManager::init(nebula::kvstore::KVStore* store) {
  if (store == nullptr) {
    return false;
  }
  if (status_.load(std::memory_order_acquire) != JbmgrStatus::NOT_START) {
    return false;
  }
  kvStore_ = store;

  status_.store(JbmgrStatus::IDLE, std::memory_order_release);
  if (handleRemainingJobs() != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }
  bgThread_ = std::thread(&JobManager::scheduleThread, this);
  LOG(INFO) << "JobManager initialized";
  return true;
}

JobManager::~JobManager() {
  shutDown();
}

nebula::cpp2::ErrorCode JobManager::handleRemainingJobs() {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode =
      kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, MetaKeyUtils::jobPrefix(), &iter);
  if (retCode == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
    LOG(INFO) << "Not leader, skip reading remaining jobs";
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't find jobs, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  std::vector<JobDescription> jds;
  for (; iter->valid(); iter->next()) {
    if (!MetaKeyUtils::isJobKey(iter->key())) {
      continue;
    }
    auto optJobRet = JobDescription::makeJobDescription(iter->key(), iter->val());
    if (nebula::ok(optJobRet)) {
      auto optJob = nebula::value(optJobRet);
      std::unique_ptr<JobExecutor> je =
          JobExecutorFactory::createJobExecutor(optJob, kvStore_, adminClient_);
      // Only balance has been recovered
      if (optJob.getStatus() == cpp2::JobStatus::RUNNING && je->isMetaJob()) {
        jds.emplace_back(optJob);
      }
    }
  }
  for (auto& jd : jds) {
    jd.setStatus(cpp2::JobStatus::QUEUE, true);
    auto jobKey = MetaKeyUtils::jobKey(jd.getSpace(), jd.getJobId());
    auto jobVal = MetaKeyUtils::jobVal(jd.getJobType(),
                                       jd.getParas(),
                                       jd.getStatus(),
                                       jd.getStartTime(),
                                       jd.getStopTime(),
                                       jd.getErrorCode());
    save(jobKey, jobVal);
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void JobManager::shutDown() {
  LOG(INFO) << "JobManager::shutDown() begin";
  // In case of shutdown more than once
  if (status_.load(std::memory_order_acquire) == JbmgrStatus::STOPPED) {
    LOG(INFO) << "JobManager not running, exit";
    return;
  }
  status_.store(JbmgrStatus::STOPPED, std::memory_order_release);
  bgThread_.join();
  LOG(INFO) << "JobManager::shutDown() end";
}

void JobManager::scheduleThread() {
  LOG(INFO) << "JobManager::scheduleThread enter";
  while (status_.load(std::memory_order_acquire) != JbmgrStatus::STOPPED) {
    std::tuple<JbOp, JobID, GraphSpaceID> opJobId;
    while (!tryDequeue(opJobId)) {
      if (status_.load(std::memory_order_acquire) == JbmgrStatus::STOPPED) {
        LOG(INFO) << "Detect shutdown called, exit";
        break;
      }
      usleep(FLAGS_job_check_intervals);
    }

    auto jobOp = std::get<0>(opJobId);
    auto jodId = std::get<1>(opJobId);
    auto spaceId = std::get<2>(opJobId);
    std::lock_guard<std::recursive_mutex> lk(muJobFinished_[spaceId]);
    auto jobDescRet = JobDescription::loadJobDescription(spaceId, jodId, kvStore_);
    if (!nebula::ok(jobDescRet)) {
      LOG(INFO) << "Load an invalid job from space " << spaceId << " jodId " << jodId;
      continue;  // leader change or archive happened
    }
    auto jobDesc = nebula::value(jobDescRet);
    if (!jobDesc.setStatus(cpp2::JobStatus::RUNNING, jobOp == JbOp::RECOVER)) {
      LOG(INFO) << "Skip job space " << spaceId << " jodId " << jodId;
      continue;
    }

    auto jobKey = MetaKeyUtils::jobKey(jobDesc.getSpace(), jobDesc.getJobId());
    auto jobVal = MetaKeyUtils::jobVal(jobDesc.getJobType(),
                                       jobDesc.getParas(),
                                       jobDesc.getStatus(),
                                       jobDesc.getStartTime(),
                                       jobDesc.getStopTime(),
                                       jobDesc.getErrorCode());
    save(jobKey, jobVal);
    spaceRunningJobs_.insert_or_assign(spaceId, true);
    if (!runJobInternal(jobDesc, jobOp)) {
      jobFinished(spaceId, jodId, cpp2::JobStatus::FAILED);
    }
  }
}

bool JobManager::runJobInternal(const JobDescription& jobDesc, JbOp op) {
  auto je = JobExecutorFactory::createJobExecutor(jobDesc, kvStore_, adminClient_);
  JobExecutor* jobExec = je.get();

  runningJobs_.emplace(jobDesc.getJobId(), std::move(je));
  if (jobExec == nullptr) {
    LOG(INFO) << "unreconized job type "
              << apache::thrift::util::enumNameSafe(jobDesc.getJobType());
    return false;
  }

  if (jobDesc.getStatus() == cpp2::JobStatus::STOPPED) {
    jobExec->stop();
    return true;
  }

  if (!jobExec->check()) {
    LOG(INFO) << "Job Executor check failed";
    return false;
  }

  if (jobExec->prepare() != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Job Executor prepare failed";
    return false;
  }
  if (op == JbOp::RECOVER) {
    jobExec->recovery();
  }
  if (jobExec->isMetaJob()) {
    jobExec->setFinishCallBack([this, jobDesc](meta::cpp2::JobStatus status) {
      if (status == meta::cpp2::JobStatus::STOPPED) {
        auto space = jobDesc.getSpace();
        std::lock_guard<std::recursive_mutex> lkg(muJobFinished_[space]);
        cleanJob(jobDesc.getJobId());
        return nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        return jobFinished(jobDesc.getSpace(), jobDesc.getJobId(), status);
      }
    });
  }
  if (jobExec->execute() != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Job dispatch failed";
    return false;
  }
  return true;
}

void JobManager::cleanJob(JobID jobId) {
  // Delete the job after job finished or failed
  LOG(INFO) << "cleanJob " << jobId;
  auto it = inFlightJobs_.find(jobId);
  if (it != inFlightJobs_.end()) {
    inFlightJobs_.erase(it);
  }
  auto itr = runningJobs_.find(jobId);
  if (itr != runningJobs_.end()) {
    runningJobs_.erase(itr);
  }
}

nebula::cpp2::ErrorCode JobManager::jobFinished(GraphSpaceID spaceId,
                                                JobID jobId,
                                                cpp2::JobStatus jobStatus) {
  LOG(INFO) << folly::sformat("{}, spaceId={}, jobId={}, result={}",
                              __func__,
                              spaceId,
                              jobId,
                              apache::thrift::util::enumNameSafe(jobStatus));
  // normal job finish may race to job stop
  std::lock_guard<std::recursive_mutex> lk(muJobFinished_[spaceId]);
  auto optJobDescRet = JobDescription::loadJobDescription(spaceId, jobId, kvStore_);
  if (!nebula::ok(optJobDescRet)) {
    LOG(INFO) << folly::sformat("Load job failed, spaceId={} jobId={}", spaceId, jobId);
    if (jobStatus != cpp2::JobStatus::STOPPED) {
      // there is a rare condition, that when job finished,
      // the job description is deleted(default more than a week)
      // but stop an invalid job should not set status to idle.
      spaceRunningJobs_.insert_or_assign(spaceId, false);
    }
    return nebula::error(optJobDescRet);
  }

  auto optJobDesc = nebula::value(optJobDescRet);

  if (!optJobDesc.setStatus(jobStatus)) {
    // job already been set as finished, failed or stopped
    return nebula::cpp2::ErrorCode::E_SAVE_JOB_FAILURE;
  }

  // Set the errorcode of the job
  nebula::cpp2::ErrorCode jobErrCode = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (jobStatus == cpp2::JobStatus::FAILED) {
    // Traverse the tasks and find the first task errorcode unsuccessful
    auto jobKey = MetaKeyUtils::jobKey(spaceId, jobId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobKey, &iter);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return rc;
    }
    for (; iter->valid(); iter->next()) {
      if (MetaKeyUtils::isJobKey(iter->key())) {
        continue;
      }
      auto tupTaskVal = MetaKeyUtils::parseTaskVal(iter->val());
      jobErrCode = std::get<4>(tupTaskVal);
      if (jobErrCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        break;
      }
    }
  }
  optJobDesc.setErrorCode(jobErrCode);

  spaceRunningJobs_.insert_or_assign(spaceId, false);
  auto jobKey = MetaKeyUtils::jobKey(optJobDesc.getSpace(), optJobDesc.getJobId());
  auto jobVal = MetaKeyUtils::jobVal(optJobDesc.getJobType(),
                                     optJobDesc.getParas(),
                                     optJobDesc.getStatus(),
                                     optJobDesc.getStartTime(),
                                     optJobDesc.getStopTime(),
                                     optJobDesc.getErrorCode());
  auto rc = save(jobKey, jobVal);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }

  auto it = runningJobs_.find(jobId);
  if (it == runningJobs_.end()) {
    // the job has not started yet
    // TODO job not existing in runningJobs_ also means leader changed, we handle it later
    cleanJob(jobId);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  std::unique_ptr<JobExecutor>& jobExec = it->second;
  if (jobStatus == cpp2::JobStatus::STOPPED) {
    jobExec->stop();
    if (!jobExec->isMetaJob()) {
      cleanJob(jobId);
    }
  } else {
    jobExec->finish(jobStatus == cpp2::JobStatus::FINISHED);
    cleanJob(jobId);
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode JobManager::saveTaskStatus(TaskDescription& td,
                                                   const cpp2::ReportTaskReq& req) {
  auto code = req.get_code();
  auto status = code == nebula::cpp2::ErrorCode::SUCCEEDED ? cpp2::JobStatus::FINISHED
                                                           : cpp2::JobStatus::FAILED;
  td.setStatus(status);
  td.setErrorCode(code);

  auto spaceId = req.get_space_id();
  auto jobId = req.get_job_id();
  auto optJobDescRet = JobDescription::loadJobDescription(spaceId, jobId, kvStore_);
  if (!nebula::ok(optJobDescRet)) {
    auto retCode = nebula::error(optJobDescRet);
    LOG(INFO) << "LoadJobDesc failed, jobId " << jobId
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto optJobDesc = nebula::value(optJobDescRet);
  auto jobExec = JobExecutorFactory::createJobExecutor(optJobDesc, kvStore_, adminClient_);

  if (!jobExec) {
    LOG(INFO) << folly::sformat("createJobExecutor failed(), jobId={}", jobId);
    return nebula::cpp2::ErrorCode::E_TASK_REPORT_OUT_DATE;
  }

  auto taskKey = MetaKeyUtils::taskKey(td.getSpace(), td.getJobId(), td.getTaskId());
  auto taskVal = MetaKeyUtils::taskVal(
      td.getHost(), td.getStatus(), td.getStartTime(), td.getStopTime(), td.getErrorCode());
  auto rcSave = save(taskKey, taskVal);
  if (rcSave != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rcSave;
  }

  return jobExec->saveSpecialTaskStatus(req);
}

void JobManager::compareChangeStatus(JbmgrStatus expected, JbmgrStatus desired) {
  JbmgrStatus ex = expected;
  status_.compare_exchange_strong(ex, desired, std::memory_order_acq_rel);
}

nebula::cpp2::ErrorCode JobManager::reportTaskFinish(const cpp2::ReportTaskReq& req) {
  auto spaceId = req.get_space_id();
  auto jobId = req.get_job_id();
  auto taskId = req.get_task_id();
  // only an active job manager will accept task finish report
  if (status_.load(std::memory_order_acquire) == JbmgrStatus::STOPPED ||
      status_.load(std::memory_order_acquire) == JbmgrStatus::NOT_START) {
    LOG(INFO) << folly::sformat(
        "report to an in-active job manager, spaceId={}, job={}, task={}", spaceId, jobId, taskId);
    return nebula::cpp2::ErrorCode::E_UNKNOWN;
  }
  // because the last task will update the job's status
  // tasks should report once a time
  std::lock_guard<std::mutex> lk(muReportFinish_[spaceId]);
  auto tasksRet = getAllTasks(spaceId, jobId);
  if (!nebula::ok(tasksRet)) {
    return nebula::error(tasksRet);
  }
  auto tasks = nebula::value(tasksRet);
  auto task = std::find_if(tasks.begin(), tasks.end(), [&](auto& it) {
    return it.getJobId() == jobId && it.getTaskId() == taskId;
  });
  if (task == tasks.end()) {
    LOG(INFO) << folly::sformat(
        "Report an invalid or outdate task, will ignore this report, job={}, "
        "task={}",
        jobId,
        taskId);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  auto rc = saveTaskStatus(*task, req);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }

  auto allTaskFinished = std::none_of(tasks.begin(), tasks.end(), [](auto& tsk) {
    return tsk.status_ == cpp2::JobStatus::RUNNING;
  });

  if (allTaskFinished) {
    auto jobStatus = std::all_of(tasks.begin(),
                                 tasks.end(),
                                 [](auto& tsk) { return tsk.status_ == cpp2::JobStatus::FINISHED; })
                         ? cpp2::JobStatus::FINISHED
                         : cpp2::JobStatus::FAILED;
    return jobFinished(spaceId, jobId, jobStatus);
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::list<TaskDescription>> JobManager::getAllTasks(
    GraphSpaceID spaceId, JobID jobId) {
  std::list<TaskDescription> taskDescriptions;
  auto jobKey = MetaKeyUtils::jobKey(spaceId, jobId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobKey, &iter);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }
  for (; iter->valid(); iter->next()) {
    if (MetaKeyUtils::isJobKey(iter->key())) {
      continue;
    }
    taskDescriptions.emplace_back(TaskDescription(iter->key(), iter->val()));
  }
  return taskDescriptions;
}

nebula::cpp2::ErrorCode JobManager::addJob(JobDescription& jobDesc, AdminClient* client) {
  auto spaceId = jobDesc.getSpace();
  auto jobId = jobDesc.getJobId();
  auto jobKey = MetaKeyUtils::jobKey(spaceId, jobId);
  auto jobVal = MetaKeyUtils::jobVal(jobDesc.getJobType(),
                                     jobDesc.getParas(),
                                     jobDesc.getStatus(),
                                     jobDesc.getStartTime(),
                                     jobDesc.getStopTime(),
                                     jobDesc.getErrorCode());
  auto rc = save(jobKey, jobVal);
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    enqueue(spaceId, jobId, JbOp::ADD, jobDesc.getJobType());
    inFlightJobs_.emplace(jobId, jobDesc);
  } else {
    LOG(INFO) << "Add Job Failed";
    if (rc != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      rc = nebula::cpp2::ErrorCode::E_ADD_JOB_FAILURE;
    }
    return rc;
  }
  adminClient_ = client;
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

size_t JobManager::jobSize() const {
  size_t size = 0;
  for (auto iter = priorityQueues_.begin(); iter != priorityQueues_.end(); ++iter) {
    size += iter->second->size();
  }
  return size;
}

// Execute jobs concurrently between spaces
// Execute jobs according to priority within the space
bool JobManager::tryDequeue(std::tuple<JbOp, JobID, GraphSpaceID>& opJobId) {
  for (auto queueIter = priorityQueues_.begin(); queueIter != priorityQueues_.end(); ++queueIter) {
    auto spaceId = queueIter->first;
    auto runningJobIter = spaceRunningJobs_.find(spaceId);
    if (runningJobIter != spaceRunningJobs_.end() && runningJobIter->second) {
      continue;
    }
    if (queueIter->second == nullptr) {
      spaceRunningJobs_.insert_or_assign(spaceId, false);
      continue;
    }
    auto ret = queueIter->second->try_dequeue(opJobId);
    if (ret) {
      return true;
    }
  }
  return false;
}

void JobManager::enqueue(GraphSpaceID space,
                         JobID jobId,
                         const JbOp& op,
                         const cpp2::JobType& jobType) {
  static const size_t priorityCount = 2;
  auto iter = priorityQueues_.find(space);
  if (iter == priorityQueues_.end()) {
    std::unique_ptr<PriorityQueue> priQueue = std::make_unique<PriorityQueue>(priorityCount);
    priorityQueues_.emplace(space, std::move(priQueue));
  }
  iter = priorityQueues_.find(space);
  CHECK(iter != priorityQueues_.end());
  if (jobType == cpp2::JobType::LEADER_BALANCE) {
    iter->second->at_priority(static_cast<size_t>(JbPriority::kHIGH))
        .enqueue(std::make_tuple(op, jobId, space));

  } else {
    iter->second->at_priority(static_cast<size_t>(JbPriority::kLOW))
        .enqueue(std::make_tuple(op, jobId, space));
  }
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::JobDesc>> JobManager::showJobs(
    GraphSpaceID spaceId) {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto jobPre = MetaKeyUtils::jobPrefix(spaceId);
  auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobPre, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Fetch jobs failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  int32_t lastExpiredJobId = INT_MIN;
  std::vector<std::string> expiredJobKeys;
  std::vector<cpp2::JobDesc> ret;

  for (; iter->valid(); iter->next()) {
    auto jobKey = iter->key();
    if (MetaKeyUtils::isJobKey(jobKey)) {
      auto optJobRet = JobDescription::makeJobDescription(jobKey, iter->val());
      if (!nebula::ok(optJobRet)) {
        expiredJobKeys.emplace_back(jobKey);
        continue;
      }
      auto optJob = nebula::value(optJobRet);
      // skip expired job, default 1 week
      if (isExpiredJob(optJob)) {
        lastExpiredJobId = optJob.getJobId();
        LOG(INFO) << "Remove expired job " << lastExpiredJobId;
        expiredJobKeys.emplace_back(jobKey);
        continue;
      }
      auto jobDesc = optJob.toJobDesc();
      ret.emplace_back(jobDesc);
    } else {  // iter-key() is a TaskKey
      TaskDescription task(jobKey, iter->val());
      if (task.getJobId() == lastExpiredJobId) {
        expiredJobKeys.emplace_back(jobKey);
      }
    }
  }

  retCode = removeExpiredJobs(std::move(expiredJobKeys));
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Remove Expired Jobs Failed";
    return retCode;
  }

  std::sort(ret.begin(), ret.end(), [](const auto& a, const auto& b) {
    return a.get_job_id() > b.get_job_id();
  });
  return ret;
}

bool JobManager::isExpiredJob(JobDescription& jobDesc) {
  auto status = jobDesc.getStatus();
  if (status == cpp2::JobStatus::QUEUE || status == cpp2::JobStatus::RUNNING) {
    return false;
  }
  auto jobStart = jobDesc.getStartTime();
  auto duration = std::difftime(nebula::time::WallClock::fastNowInSec(), jobStart);
  return duration > FLAGS_job_expired_secs;
}

nebula::cpp2::ErrorCode JobManager::removeExpiredJobs(
    std::vector<std::string>&& expiredJobsAndTasks) {
  nebula::cpp2::ErrorCode ret;
  folly::Baton<true, std::atomic> baton;
  kvStore_->asyncMultiRemove(kDefaultSpaceId,
                             kDefaultPartId,
                             std::move(expiredJobsAndTasks),
                             [&](nebula::cpp2::ErrorCode code) {
                               if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                                 LOG(INFO) << "kvstore asyncRemoveRange failed: "
                                           << apache::thrift::util::enumNameSafe(code);
                               }
                               ret = code;
                               baton.post();
                             });
  baton.wait();
  return ret;
}

bool JobManager::checkOnRunningJobExist(GraphSpaceID spaceId,
                                        const cpp2::JobType& jobType,
                                        const std::vector<std::string>& paras,
                                        JobID& jobId) {
  JobDescription jobDesc(spaceId, 0, jobType, paras);
  auto it = inFlightJobs_.begin();
  while (it != inFlightJobs_.end()) {
    if (it->second == jobDesc) {
      jobId = it->first;
      return true;
    }
    ++it;
  }
  return false;
}

nebula::cpp2::ErrorCode JobManager::checkNeedRecoverJobExist(GraphSpaceID spaceId,
                                                             const cpp2::JobType& jobType) {
  if (jobType == cpp2::JobType::DATA_BALANCE || jobType == cpp2::JobType::ZONE_BALANCE) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto jobPre = MetaKeyUtils::jobPrefix(spaceId);
    auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobPre, &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Fetch jobs failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    for (; iter->valid(); iter->next()) {
      if (!MetaKeyUtils::isJobKey(iter->key())) {
        continue;
      }
      auto tup = MetaKeyUtils::parseJobVal(iter->val());
      auto type = std::get<0>(tup);
      auto status = std::get<2>(tup);
      if (type == cpp2::JobType::DATA_BALANCE || type == cpp2::JobType::ZONE_BALANCE) {
        // QUEUE: The job has not been executed, the machine restarted
        if (status == cpp2::JobStatus::FAILED || status == cpp2::JobStatus::QUEUE) {
          return nebula::cpp2::ErrorCode::E_JOB_NEED_RECOVER;
        }
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>>
JobManager::showJob(GraphSpaceID spaceId, JobID jobId) {
  auto jobKey = MetaKeyUtils::jobKey(spaceId, jobId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobKey, &iter);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }

  if (!iter->valid()) {
    return nebula::cpp2::ErrorCode::E_JOB_NOT_IN_SPACE;
  }

  std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>> ret;
  for (; iter->valid(); iter->next()) {
    auto jKey = iter->key();
    if (MetaKeyUtils::isJobKey(jKey)) {
      auto optJobRet = JobDescription::makeJobDescription(jKey, iter->val());
      if (!nebula::ok(optJobRet)) {
        return nebula::error(optJobRet);
      }
      auto optJob = nebula::value(optJobRet);
      ret.first = optJob.toJobDesc();
    } else {
      TaskDescription td(jKey, iter->val());
      ret.second.emplace_back(td.toTaskDesc());
    }
  }
  if (ret.first.get_type() == meta::cpp2::JobType::DATA_BALANCE ||
      ret.first.get_type() == meta::cpp2::JobType::ZONE_BALANCE) {
    auto res = BalancePlan::show(jobId, kvStore_, adminClient_);
    if (ok(res)) {
      std::vector<cpp2::BalanceTask> thriftTasks = value(res);
      auto& vec = ret.first.paras_ref<>().value();
      size_t index = vec.size();
      for (const auto& t : thriftTasks) {
        std::string resVal;
        apache::thrift::CompactSerializer::serialize(t, &resVal);
        auto& val = ret.first.paras_ref<>().value();
        val.emplace_back(resVal);
      }
      vec.emplace_back(std::to_string(index));
    }
  }
  return ret;
}

nebula::cpp2::ErrorCode JobManager::stopJob(GraphSpaceID spaceId, JobID jobId) {
  LOG(INFO) << folly::sformat("Try to stop job {} in space {}", jobId, spaceId);
  return jobFinished(spaceId, jobId, cpp2::JobStatus::STOPPED);
}

ErrorOr<nebula::cpp2::ErrorCode, uint32_t> JobManager::recoverJob(
    GraphSpaceID spaceId, AdminClient* client, const std::vector<int32_t>& jobIds) {
  int32_t recoveredJobNum = 0;
  std::vector<std::pair<std::string, std::string>> jobKVs;
  adminClient_ = client;
  if (jobIds.empty()) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto jobPre = MetaKeyUtils::jobPrefix(spaceId);
    auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobPre, &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Can't find jobs, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }
    for (; iter->valid(); iter->next()) {
      if (!MetaKeyUtils::isJobKey(iter->key())) {
        continue;
      }
      jobKVs.emplace_back(std::make_pair(iter->key(), iter->val()));
    }
  } else {
    std::vector<std::string> jobKeys;
    jobKeys.reserve(jobIds.size());
    std::vector<std::pair<std::string, std::string>> totalJobKVs;
    for (int jobId : jobIds) {
      jobKeys.emplace_back(MetaKeyUtils::jobKey(spaceId, jobId));
    }
    std::vector<std::string> jobVals;
    auto retCode = kvStore_->multiGet(kDefaultSpaceId, kDefaultPartId, jobKeys, &jobVals);
    if (retCode.first != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Can't find jobs, error: " << apache::thrift::util::enumNameSafe(retCode.first);
      return retCode.first;
    }
    for (size_t i = 0; i < jobKeys.size(); i++) {
      totalJobKVs.emplace_back(std::make_pair(jobKeys[i], jobVals[i]));
    }

    // For DATA_BALANCE and ZONE_BALANCE job, jobs with STOPPED, FAILED, QUEUE status
    // !!! The following situations can be recovered, only for jobs of the same type
    // of DATA_BALANCE or ZONE_BALANCE。
    // QUEUE: The job has not been executed, then the machine restarted.
    // FAILED:
    // The failed job will be recovered.
    // FAILED and QUEUE jobs will not exist at the same time.
    // STOPPED:
    // If only one stopped jobId is specified, No FINISHED job or FAILED job of the
    // same type after this job.
    // If multiple jobs of the same type are specified, only starttime latest jobId
    // will can be recovered, no FINISHED job or FAILED job of the same type after this latest job.
    // The same type of STOPPED job exists in the following form, sorted by starttime:
    // STOPPED job1, FAILED job2
    // recover job job1        failed
    // recover job job2        success
    // STOPPED job1, FINISHED job2, STOPPED job3
    // recover job job1        failed
    // recover job job3        success
    // recover job job1,job3   Only job3 can recover
    std::unordered_map<cpp2::JobType, std::tuple<JobID, int64_t, cpp2::JobStatus>> dupResult;
    std::unordered_map<JobID, std::pair<std::string, std::string>> dupkeyVal;

    for (auto& jobkv : totalJobKVs) {
      auto optJobRet = JobDescription::makeJobDescription(jobkv.first, jobkv.second);
      if (nebula::ok(optJobRet)) {
        auto optJob = nebula::value(optJobRet);
        auto jobStatus = optJob.getStatus();
        auto jobId = optJob.getJobId();
        auto jobType = optJob.getJobType();
        auto jobStartTime = optJob.getStartTime();
        if (jobStatus != cpp2::JobStatus::QUEUE && jobStatus != cpp2::JobStatus::FAILED &&
            jobStatus != cpp2::JobStatus::STOPPED) {
          continue;
        }

        // handle DATA_BALANCE and ZONE_BALANCE
        if (jobType == cpp2::JobType::DATA_BALANCE || jobType == cpp2::JobType::ZONE_BALANCE) {
          // FAILED and QUEUE jobs will not exist at the same time.
          if (jobStatus == cpp2::JobStatus::FAILED || jobStatus == cpp2::JobStatus::QUEUE) {
            dupResult[jobType] = std::make_tuple(jobId, jobStartTime, jobStatus);
            dupkeyVal.emplace(jobId, std::make_pair(jobkv.first, jobkv.second));
            continue;
          }

          // current recover job status is stopped
          auto findJobIter = dupResult.find(jobType);
          if (findJobIter != dupResult.end()) {
            auto oldJobInfo = findJobIter->second;
            if (std::get<2>(oldJobInfo) != cpp2::JobStatus::STOPPED) {
              continue;
            }
          }

          // For a stopped job, check whether there is the same type of finished or
          // failed job after it.
          std::unique_ptr<kvstore::KVIterator> iter;
          auto jobPre = MetaKeyUtils::jobPrefix(spaceId);
          auto code = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobPre, &iter);
          if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(INFO) << "Fetch jobs failed, error: " << apache::thrift::util::enumNameSafe(code);
            return code;
          }

          bool findRest = false;
          for (; iter->valid(); iter->next()) {
            if (!MetaKeyUtils::isJobKey(iter->key())) {
              continue;
            }

            // eliminate oneself
            auto keyPair = MetaKeyUtils::parseJobKey(iter->key());
            auto destJobId = keyPair.second;
            if (destJobId == jobId) {
              continue;
            }
            auto tup = MetaKeyUtils::parseJobVal(iter->val());
            auto destJobType = std::get<0>(tup);
            auto destJobStatus = std::get<2>(tup);
            auto destJobStartTime = std::get<3>(tup);
            if (jobType == destJobType) {
              // There is a specific type of failed job that does not allow recovery for the type of
              // stopped job
              if (destJobStatus == cpp2::JobStatus::FAILED) {
                LOG(ERROR) << "There is a specific type of failed job that does not allow recovery "
                              "for the type of stopped job";
                findRest = true;
                break;
              } else if (destJobStatus == cpp2::JobStatus::FINISHED) {
                // Compare the start time of the job
                if (destJobStartTime > jobStartTime) {
                  findRest = true;
                  break;
                }
              }
            }
          }
          if (!findRest) {
            auto findStoppedJobIter = dupResult.find(jobType);
            if (findStoppedJobIter != dupResult.end()) {
              // update stopped job
              auto oldJobInfo = findStoppedJobIter->second;
              auto oldJobStartTime = std::get<1>(oldJobInfo);
              if (jobStartTime > oldJobStartTime) {
                auto oldJobId = std::get<0>(oldJobInfo);
                dupResult[jobType] = std::make_tuple(jobId, jobStartTime, jobStatus);
                dupkeyVal.erase(oldJobId);
                dupkeyVal.emplace(jobId, std::make_pair(jobkv.first, jobkv.second));
              }
            } else {
              // insert
              dupResult[jobType] = std::make_tuple(jobId, jobStartTime, jobStatus);
              dupkeyVal.emplace(jobId, std::make_pair(jobkv.first, jobkv.second));
            }
          }
        } else {
          jobKVs.emplace_back(std::make_pair(jobkv.first, jobkv.second));
        }
      }
    }
    for (auto& key : dupResult) {
      auto jId = std::get<0>(key.second);
      jobKVs.emplace_back(dupkeyVal[jId]);
    }
  }

  for (auto& jobkv : jobKVs) {
    auto optJobRet = JobDescription::makeJobDescription(jobkv.first, jobkv.second);
    if (nebula::ok(optJobRet)) {
      auto optJob = nebula::value(optJobRet);
      if (optJob.getStatus() == cpp2::JobStatus::QUEUE ||
          (jobIds.size() && (optJob.getStatus() == cpp2::JobStatus::FAILED ||
                             optJob.getStatus() == cpp2::JobStatus::STOPPED))) {
        // Check if the job exists
        JobID jId = 0;
        auto jobExist =
            checkOnRunningJobExist(spaceId, optJob.getJobType(), optJob.getParas(), jId);
        if (!jobExist) {
          auto jobId = optJob.getJobId();
          enqueue(spaceId, jobId, JbOp::RECOVER, optJob.getJobType());
          inFlightJobs_.emplace(jobId, optJob);
          ++recoveredJobNum;
        }
      }
    }
  }
  return recoveredJobNum;
}

nebula::cpp2::ErrorCode JobManager::save(const std::string& k, const std::string& v) {
  std::vector<kvstore::KV> data{std::make_pair(k, v)};
  folly::Baton<true, std::atomic> baton;
  nebula::cpp2::ErrorCode rc = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvStore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        rc = code;
        baton.post();
      });
  baton.wait();
  return rc;
}

ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> JobManager::getSpaceId(const std::string& name) {
  auto indexKey = MetaKeyUtils::indexSpaceKey(name);
  std::string val;
  auto retCode = kvStore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &val);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    LOG(INFO) << "KVStore error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  return *reinterpret_cast<const GraphSpaceID*>(val.c_str());
}

ErrorOr<nebula::cpp2::ErrorCode, bool> JobManager::checkTypeJobRunning(
    std::unordered_set<cpp2::JobType>& jobTypes) {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto jobPrefix = MetaKeyUtils::jobPrefix();
  auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobPrefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Fetch Jobs Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  for (; iter->valid(); iter->next()) {
    auto jobKey = iter->key();
    if (MetaKeyUtils::isJobKey(jobKey)) {
      auto optJobRet = JobDescription::makeJobDescription(jobKey, iter->val());
      if (!nebula::ok(optJobRet)) {
        continue;
      }
      auto jobDesc = nebula::value(optJobRet);
      auto jType = jobDesc.getJobType();
      if (jobTypes.find(jType) == jobTypes.end()) {
        continue;
      }

      auto status = jobDesc.getStatus();
      if (status == cpp2::JobStatus::QUEUE || status == cpp2::JobStatus::RUNNING) {
        return true;
      }
    }
  }

  return false;
}

}  // namespace meta
}  // namespace nebula
