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
#include "meta/processors/job/JobUtils.h"
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

  lowPriorityQueue_ = std::make_unique<folly::UMPSCQueue<std::pair<JbOp, JobID>, true>>();
  highPriorityQueue_ = std::make_unique<folly::UMPSCQueue<std::pair<JbOp, JobID>, true>>();

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
  auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
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
    if (!JobDescription::isJobKey(iter->key())) {
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
    save(jd.jobKey(), jd.jobVal());
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void JobManager::shutDown() {
  LOG(INFO) << "JobManager::shutDown() begin";
  if (status_.load(std::memory_order_acquire) ==
      JbmgrStatus::STOPPED) {  // in case of shutdown more than once
    LOG(INFO) << "JobManager not running, exit";
    return;
  }
  status_.store(JbmgrStatus::STOPPED, std::memory_order_release);
  bgThread_.join();
  LOG(INFO) << "JobManager::shutDown() end";
}

void JobManager::scheduleThread() {
  LOG(INFO) << "JobManager::runJobBackground() enter";
  while (status_.load(std::memory_order_acquire) != JbmgrStatus::STOPPED) {
    std::pair<JbOp, JobID> opJobId;
    while (status_.load(std::memory_order_acquire) == JbmgrStatus::BUSY || !try_dequeue(opJobId)) {
      if (status_.load(std::memory_order_acquire) == JbmgrStatus::STOPPED) {
        LOG(INFO) << "[JobManager] detect shutdown called, exit";
        break;
      }
      usleep(FLAGS_job_check_intervals);
    }

    auto jobDescRet = JobDescription::loadJobDescription(opJobId.second, kvStore_);
    if (!nebula::ok(jobDescRet)) {
      LOG(INFO) << "[JobManager] load an invalid job from queue " << opJobId.second;
      continue;  // leader change or archive happened
    }
    auto jobDesc = nebula::value(jobDescRet);
    if (!jobDesc.setStatus(cpp2::JobStatus::RUNNING, opJobId.first == JbOp::RECOVER)) {
      LOG(INFO) << "[JobManager] skip job " << opJobId.second;
      continue;
    }
    save(jobDesc.jobKey(), jobDesc.jobVal());
    compareChangeStatus(JbmgrStatus::IDLE, JbmgrStatus::BUSY);
    if (!runJobInternal(jobDesc, opJobId.first)) {
      jobFinished(opJobId.second, cpp2::JobStatus::FAILED);
    }
  }
}

bool JobManager::runJobInternal(const JobDescription& jobDesc, JbOp op) {
  std::lock_guard<std::recursive_mutex> lk(muJobFinished_);
  std::unique_ptr<JobExecutor> je =
      JobExecutorFactory::createJobExecutor(jobDesc, kvStore_, adminClient_);
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
        std::lock_guard<std::recursive_mutex> lkg(muJobFinished_);
        cleanJob(jobDesc.getJobId());
        return nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        return jobFinished(jobDesc.getJobId(), status);
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
  LOG(INFO) << "[task] cleanJob " << jobId;
  auto it = inFlightJobs_.find(jobId);
  if (it != inFlightJobs_.end()) {
    inFlightJobs_.erase(it);
  }
  auto itr = runningJobs_.find(jobId);
  if (itr != runningJobs_.end()) {
    runningJobs_.erase(itr);
  }
}

nebula::cpp2::ErrorCode JobManager::jobFinished(JobID jobId, cpp2::JobStatus jobStatus) {
  LOG(INFO) << folly::sformat(
      "{}, jobId={}, result={}", __func__, jobId, apache::thrift::util::enumNameSafe(jobStatus));
  // normal job finish may race to job stop
  std::lock_guard<std::recursive_mutex> lk(muJobFinished_);
  auto optJobDescRet = JobDescription::loadJobDescription(jobId, kvStore_);
  if (!nebula::ok(optJobDescRet)) {
    LOG(INFO) << folly::sformat("can't load job, jobId={}", jobId);
    if (jobStatus != cpp2::JobStatus::STOPPED) {
      // there is a rare condition, that when job finished,
      // the job description is deleted(default more than a week)
      // but stop an invalid job should not set status to idle.
      compareChangeStatus(JbmgrStatus::BUSY, JbmgrStatus::IDLE);
    }
    return nebula::error(optJobDescRet);
  }

  auto optJobDesc = nebula::value(optJobDescRet);

  if (!optJobDesc.setStatus(jobStatus)) {
    // job already been set as finished, failed or stopped
    return nebula::cpp2::ErrorCode::E_SAVE_JOB_FAILURE;
  }
  compareChangeStatus(JbmgrStatus::BUSY, JbmgrStatus::IDLE);
  auto rc = save(optJobDesc.jobKey(), optJobDesc.jobVal());
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }

  auto it = runningJobs_.find(jobId);
  if (it == runningJobs_.end()) {
    LOG(INFO) << folly::sformat("can't find jobExecutor, jobId={}", jobId);
    return nebula::cpp2::ErrorCode::E_UNKNOWN;
  }
  std::unique_ptr<JobExecutor>& jobExec = it->second;
  if (!optJobDesc.getParas().empty()) {
    auto spaceName = optJobDesc.getParas().back();
    auto spaceIdRet = getSpaceId(spaceName);
    if (!nebula::ok(spaceIdRet)) {
      auto retCode = nebula::error(spaceIdRet);
      LOG(INFO) << "Get spaceName " << spaceName
                << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    auto spaceId = nebula::value(spaceIdRet);
    if (spaceId == -1) {
      return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }
    jobExec->setSpaceId(spaceId);
  }
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

  auto jobId = req.get_job_id();
  auto optJobDescRet = JobDescription::loadJobDescription(jobId, kvStore_);
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

  auto rcSave = save(td.taskKey(), td.taskVal());
  if (rcSave != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rcSave;
  }

  if (!optJobDesc.getParas().empty()) {
    GraphSpaceID spaceId = -1;
    auto spaceName = optJobDesc.getParas().back();
    auto spaceIdRet = getSpaceId(spaceName);
    if (!nebula::ok(spaceIdRet)) {
      auto retCode = nebula::error(spaceIdRet);
      LOG(INFO) << "Get spaceName " << spaceName
                << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    } else {
      spaceId = nebula::value(spaceIdRet);
      jobExec->setSpaceId(spaceId);
    }
  }

  return jobExec->saveSpecialTaskStatus(req);
}

void JobManager::compareChangeStatus(JbmgrStatus expected, JbmgrStatus desired) {
  JbmgrStatus ex = expected;
  status_.compare_exchange_strong(ex, desired, std::memory_order_acq_rel);
}

/**
 * @brief
 *      client should retry if any persist attempt
 *      for example leader change / store failure.
 *      else, may log then ignore error
 * @return cpp2::ErrorCode
 */
nebula::cpp2::ErrorCode JobManager::reportTaskFinish(const cpp2::ReportTaskReq& req) {
  auto jobId = req.get_job_id();
  auto taskId = req.get_task_id();
  // only an active job manager will accept task finish report
  if (status_.load(std::memory_order_acquire) == JbmgrStatus::STOPPED ||
      status_.load(std::memory_order_acquire) == JbmgrStatus::NOT_START) {
    LOG(INFO) << folly::sformat(
        "report to an in-active job manager, job={}, task={}", jobId, taskId);
    return nebula::cpp2::ErrorCode::E_UNKNOWN;
  }
  // because the last task will update the job's status
  // tasks should report once a time
  std::lock_guard<std::mutex> lk(muReportFinish_);
  auto tasksRet = getAllTasks(jobId);
  if (!nebula::ok(tasksRet)) {
    return nebula::error(tasksRet);
  }
  auto tasks = nebula::value(tasksRet);
  auto task = std::find_if(tasks.begin(), tasks.end(), [&](auto& it) {
    return it.getJobId() == jobId && it.getTaskId() == taskId;
  });
  if (task == tasks.end()) {
    LOG(INFO) << folly::sformat(
        "report an invalid or outdate task, will ignore this report, job={}, "
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
    return jobFinished(jobId, jobStatus);
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::list<TaskDescription>> JobManager::getAllTasks(JobID jobId) {
  std::list<TaskDescription> taskDescriptions;
  auto jobKey = JobDescription::makeJobKey(jobId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobKey, &iter);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }
  for (; iter->valid(); iter->next()) {
    if (JobDescription::isJobKey(iter->key())) {
      continue;
    }
    taskDescriptions.emplace_back(TaskDescription(iter->key(), iter->val()));
  }
  return taskDescriptions;
}

nebula::cpp2::ErrorCode JobManager::addJob(const JobDescription& jobDesc, AdminClient* client) {
  auto rc = save(jobDesc.jobKey(), jobDesc.jobVal());
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    auto jobId = jobDesc.getJobId();
    enqueue(JbOp::ADD, jobId, jobDesc.getJobType());
    // Add job to jobMap
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
  return highPriorityQueue_->size() + lowPriorityQueue_->size();
}

bool JobManager::try_dequeue(std::pair<JbOp, JobID>& opJobId) {
  if (highPriorityQueue_->try_dequeue(opJobId)) {
    return true;
  } else if (lowPriorityQueue_->try_dequeue(opJobId)) {
    return true;
  }
  return false;
}

void JobManager::enqueue(const JbOp& op, const JobID& jobId, const cpp2::JobType& jobType) {
  if (jobType == cpp2::JobType::STATS) {
    highPriorityQueue_->enqueue(std::make_pair(op, jobId));
  } else {
    lowPriorityQueue_->enqueue(std::make_pair(op, jobId));
  }
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::JobDesc>> JobManager::showJobs(
    const std::string& spaceName) {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Fetch Jobs Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  int32_t lastExpiredJobId = INT_MIN;
  std::vector<std::string> expiredJobKeys;
  std::vector<cpp2::JobDesc> ret;

  for (; iter->valid(); iter->next()) {
    auto jobKey = iter->key();
    if (JobDescription::isJobKey(jobKey)) {
      auto optJobRet = JobDescription::makeJobDescription(jobKey, iter->val());
      if (!nebula::ok(optJobRet)) {
        expiredJobKeys.emplace_back(jobKey);
        continue;
      }
      auto optJob = nebula::value(optJobRet);
      // skip expired job, default 1 week
      auto jobDesc = optJob.toJobDesc();
      if (isExpiredJob(jobDesc)) {
        lastExpiredJobId = jobDesc.get_id();
        LOG(INFO) << "remove expired job " << lastExpiredJobId;
        expiredJobKeys.emplace_back(jobKey);
        continue;
      }
      if (jobDesc.get_paras().back() != spaceName) {
        continue;
      }
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

  std::sort(
      ret.begin(), ret.end(), [](const auto& a, const auto& b) { return a.get_id() > b.get_id(); });
  return ret;
}

bool JobManager::isExpiredJob(const cpp2::JobDesc& jobDesc) {
  if (*jobDesc.status_ref() == cpp2::JobStatus::QUEUE ||
      *jobDesc.status_ref() == cpp2::JobStatus::RUNNING) {
    return false;
  }
  auto jobStart = jobDesc.get_start_time();
  auto duration = std::difftime(nebula::time::WallClock::fastNowInSec(), jobStart);
  return duration > FLAGS_job_expired_secs;
}

bool JobManager::isRunningJob(const JobDescription& jobDesc) {
  auto status = jobDesc.getStatus();
  return status == cpp2::JobStatus::QUEUE || status == cpp2::JobStatus::RUNNING;
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

bool JobManager::checkJobExist(const cpp2::JobType& jobType,
                               const std::vector<std::string>& paras,
                               JobID& iJob) {
  JobDescription jobDesc(0, jobType, paras);
  auto it = inFlightJobs_.begin();
  while (it != inFlightJobs_.end()) {
    if (it->second == jobDesc) {
      iJob = it->first;
      return true;
    }
    ++it;
  }
  return false;
}

ErrorOr<nebula::cpp2::ErrorCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>>
JobManager::showJob(JobID iJob, const std::string& spaceName) {
  auto jobKey = JobDescription::makeJobKey(iJob);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobKey, &iter);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }

  if (!iter->valid()) {
    return nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
  }

  std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>> ret;
  for (; iter->valid(); iter->next()) {
    auto jKey = iter->key();
    if (JobDescription::isJobKey(jKey)) {
      auto optJobRet = JobDescription::makeJobDescription(jKey, iter->val());
      if (!nebula::ok(optJobRet)) {
        return nebula::error(optJobRet);
      }
      auto optJob = nebula::value(optJobRet);
      if (optJob.getParas().back() != spaceName) {
        LOG(INFO) << "Show job " << iJob << " not in current space " << spaceName;
        return nebula::cpp2::ErrorCode::E_JOB_NOT_IN_SPACE;
      }
      ret.first = optJob.toJobDesc();
    } else {
      TaskDescription td(jKey, iter->val());
      ret.second.emplace_back(td.toTaskDesc());
    }
  }
  if (ret.first.get_type() == meta::cpp2::JobType::DATA_BALANCE ||
      ret.first.get_type() == meta::cpp2::JobType::ZONE_BALANCE) {
    auto res = BalancePlan::show(iJob, kvStore_, adminClient_);
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

nebula::cpp2::ErrorCode JobManager::stopJob(JobID iJob, const std::string& spaceName) {
  LOG(INFO) << "try to stop job " << iJob;
  auto optJobDescRet = JobDescription::loadJobDescription(iJob, kvStore_);
  if (!nebula::ok(optJobDescRet)) {
    auto retCode = nebula::error(optJobDescRet);
    LOG(INFO) << "LoadJobDesc failed, jobId " << iJob
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto optJobDesc = nebula::value(optJobDescRet);
  if (optJobDesc.getParas().back() != spaceName) {
    LOG(INFO) << "Stop job " << iJob << " not in space " << spaceName;
    return nebula::cpp2::ErrorCode::E_JOB_NOT_IN_SPACE;
  }
  return jobFinished(iJob, cpp2::JobStatus::STOPPED);
}

/*
 * Return: recovered job num.
 * */
ErrorOr<nebula::cpp2::ErrorCode, uint32_t> JobManager::recoverJob(
    const std::string& spaceName, AdminClient* client, const std::vector<int32_t>& jobIds) {
  int32_t recoveredJobNum = 0;
  std::vector<std::pair<std::string, std::string>> kvs;
  adminClient_ = client;
  if (jobIds.empty()) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Can't find jobs, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }
    for (; iter->valid(); iter->next()) {
      if (!JobDescription::isJobKey(iter->key())) {
        continue;
      }
      kvs.emplace_back(std::make_pair(iter->key(), iter->val()));
    }
  } else {
    std::vector<std::string> keys;
    keys.reserve(jobIds.size());
    for (int jobId : jobIds) {
      keys.emplace_back(JobDescription::makeJobKey(jobId));
    }
    std::vector<std::string> values;
    auto retCode = kvStore_->multiGet(kDefaultSpaceId, kDefaultPartId, keys, &values);
    if (retCode.first != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Can't find jobs, error: " << apache::thrift::util::enumNameSafe(retCode.first);
      return retCode.first;
    }
    for (size_t i = 0; i < keys.size(); i++) {
      kvs.emplace_back(std::make_pair(keys[i], values[i]));
    }
  }

  for (const std::pair<std::string, std::string>& p : kvs) {
    auto optJobRet = JobDescription::makeJobDescription(p.first, p.second);
    if (nebula::ok(optJobRet)) {
      auto optJob = nebula::value(optJobRet);
      if (optJob.getParas().back() != spaceName) {
        continue;
      }
      if (optJob.getStatus() == cpp2::JobStatus::QUEUE ||
          (jobIds.size() && (optJob.getStatus() == cpp2::JobStatus::FAILED ||
                             optJob.getStatus() == cpp2::JobStatus::STOPPED))) {
        // Check if the job exists
        JobID jId = 0;
        auto jobExist = checkJobExist(optJob.getJobType(), optJob.getParas(), jId);

        if (!jobExist) {
          auto jobId = optJob.getJobId();
          enqueue(JbOp::RECOVER, jobId, optJob.getJobType());
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
  auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
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

ErrorOr<nebula::cpp2::ErrorCode, bool> JobManager::checkIndexJobRunning() {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Fetch Jobs Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  for (; iter->valid(); iter->next()) {
    auto jobKey = iter->key();
    if (JobDescription::isJobKey(jobKey)) {
      auto optJobRet = JobDescription::makeJobDescription(jobKey, iter->val());
      if (!nebula::ok(optJobRet)) {
        continue;
      }
      auto jobDesc = nebula::value(optJobRet);
      if (!isRunningJob(jobDesc)) {
        continue;
      }
      auto jobType = jobDesc.getJobType();
      if (jobType == cpp2::JobType::REBUILD_TAG_INDEX ||
          jobType == cpp2::JobType::REBUILD_EDGE_INDEX) {
        return true;
      }
    }
  }

  return false;
}

}  // namespace meta
}  // namespace nebula
