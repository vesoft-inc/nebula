/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/JobManager.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
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
DEFINE_int32(job_threads, 10, "Threads number for exec job");

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
  executor_.reset(new folly::CPUThreadPoolExecutor(FLAGS_job_threads));
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
      // Only balance would change
      if (optJob.getStatus() == cpp2::JobStatus::RUNNING && je->isMetaJob()) {
        jds.emplace_back(std::move(optJob));
      } else if (optJob.getStatus() == cpp2::JobStatus::QUEUE) {
        auto mutexIter = muJobFinished_.find(optJob.getSpace());
        if (mutexIter == muJobFinished_.end()) {
          mutexIter =
              muJobFinished_.emplace(optJob.getSpace(), std::make_unique<std::recursive_mutex>())
                  .first;
        }
        std::lock_guard<std::recursive_mutex> lk(*(mutexIter->second));
        auto spaceId = optJob.getSpace();
        auto jobId = optJob.getJobId();
        enqueue(spaceId, jobId, JbOp::ADD, optJob.getJobType());
        inFlightJobs_.emplace(std::move(jobId), std::move(optJob));
      }
    }
  }
  for (auto& jd : jds) {
    jd.setStatus(cpp2::JobStatus::FAILED, true);
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
    JobDescription jobDesc;
    {
      auto iter = muJobFinished_.find(spaceId);
      if (iter == muJobFinished_.end()) {
        iter = muJobFinished_.emplace(spaceId, std::make_unique<std::recursive_mutex>()).first;
      }

      std::lock_guard<std::recursive_mutex> lk(*(iter->second));

      auto jobDescRet = JobDescription::loadJobDescription(spaceId, jodId, kvStore_);
      if (!nebula::ok(jobDescRet)) {
        LOG(INFO) << "Load an invalid job from space " << spaceId << " jodId " << jodId;
        continue;  // leader change or archive happened
      }
      jobDesc = nebula::value(jobDescRet);
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
    }

    runJobInternal(jobDesc, jobOp).via(executor_.get()).thenValue([](auto&&) {});
  }
}

nebula::cpp2::ErrorCode JobManager::prepareRunJob(JobExecutor* jobExec,
                                                  const JobDescription& jobDesc,
                                                  JbOp op) {
  if (jobExec == nullptr) {
    LOG(INFO) << "Unreconized job type "
              << apache::thrift::util::enumNameSafe(jobDesc.getJobType());
    return nebula::cpp2::ErrorCode::E_ADD_JOB_FAILURE;
  }

  auto code = jobExec->check();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Job Executor check failed";
    return code;
  }

  code = jobExec->prepare();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Job Executor prepare failed";
    return code;
  }
  if (op == JbOp::RECOVER) {
    code = jobExec->recovery();
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Recover job failed";
      return code;
    }
  }
  return code;
}

folly::Future<nebula::cpp2::ErrorCode> JobManager::runJobInternal(const JobDescription& jobDesc,
                                                                  JbOp op) {
  folly::Promise<nebula::cpp2::ErrorCode> promise;
  auto retFuture = promise.getFuture();
  JobExecutor* jobExec;
  auto spaceId = jobDesc.getSpace();

  {
    auto iter = this->muJobFinished_.find(spaceId);
    if (iter == this->muJobFinished_.end()) {
      iter = this->muJobFinished_.emplace(spaceId, std::make_unique<std::recursive_mutex>()).first;
    }
    std::lock_guard<std::recursive_mutex> lk(*(iter->second));
    auto je = JobExecutorFactory::createJobExecutor(jobDesc, kvStore_, adminClient_);
    jobExec = je.get();

    runningJobs_.emplace(jobDesc.getJobId(), std::move(je));
    auto retCode = prepareRunJob(jobExec, jobDesc, op);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      jobFinished(spaceId, jobDesc.getJobId(), cpp2::JobStatus::FAILED, retCode);
      resetSpaceRunning(spaceId);
      promise.setValue(retCode);
      return retFuture;
    }
  }

  jobExec->execute().thenValue([jobExec, pro = std::move(promise), this](
                                   nebula::cpp2::ErrorCode&& code) mutable {
    auto isMetaJob = jobExec->isMetaJob();
    auto jDesc = jobExec->getJobDescription();
    auto jStatus = jDesc.getStatus();
    auto jSpace = jDesc.getSpace();
    auto jJob = jDesc.getJobId();

    auto iter = this->muJobFinished_.find(jSpace);
    if (iter == this->muJobFinished_.end()) {
      iter = this->muJobFinished_.emplace(jSpace, std::make_unique<std::recursive_mutex>()).first;
    }
    std::lock_guard<std::recursive_mutex> lk(*(iter->second));
    jobExec->resetRunningStatus();

    if (isMetaJob) {
      if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        if (jStatus == meta::cpp2::JobStatus::STOPPED) {
          cleanJob(jJob);
        } else {
          jobFinished(jSpace, jJob, jStatus);
        }
      } else {
        jobFinished(jSpace, jJob, cpp2::JobStatus::FAILED, code);
      }
      resetSpaceRunning(jSpace);
    } else {
      // storage job
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        jobFinished(jSpace, jJob, cpp2::JobStatus::FAILED, code);
        resetSpaceRunning(jSpace);
      }
    }
    pro.setValue(std::move(code));
  });
  return retFuture;
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

nebula::cpp2::ErrorCode JobManager::jobFinished(
    GraphSpaceID spaceId,
    JobID jobId,
    cpp2::JobStatus jobStatus,
    std::optional<nebula::cpp2::ErrorCode> jobErrorCode) {
  LOG(INFO) << folly::sformat("{}, spaceId={}, jobId={}, result={}",
                              __func__,
                              spaceId,
                              jobId,
                              apache::thrift::util::enumNameSafe(jobStatus));
  DCHECK(jobStatus == cpp2::JobStatus::FINISHED || jobStatus == cpp2::JobStatus::FAILED ||
         jobStatus == cpp2::JobStatus::STOPPED);
  // normal job finish may race to job stop
  auto mutexIter = muJobFinished_.find(spaceId);
  if (mutexIter == muJobFinished_.end()) {
    mutexIter = muJobFinished_.emplace(spaceId, std::make_unique<std::recursive_mutex>()).first;
  }

  std::lock_guard<std::recursive_mutex> lk(*(mutexIter->second));

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

  bool force = false;
  if (optJobDesc.getStatus() == cpp2::JobStatus::FAILED && jobStatus == cpp2::JobStatus::STOPPED &&
      (optJobDesc.getJobType() == cpp2::JobType::ZONE_BALANCE ||
       optJobDesc.getJobType() == cpp2::JobType::DATA_BALANCE)) {
    force = true;
  }

  if (!optJobDesc.setStatus(jobStatus, force)) {
    // job already been set as finished, failed or stopped
    return nebula::cpp2::ErrorCode::E_JOB_ALREADY_FINISH;
  }

  // If the job is marked as FAILED, one of the following will be triggered
  // 1. If any of the task failed, set the errorcode of the job to the failed task code.
  // 2. The job failed before running any task (e.g. in check or prepare), the error code of the
  // job will be set as it
  if (jobStatus == cpp2::JobStatus::FAILED) {
    if (!jobErrorCode.has_value()) {
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
        auto taskErrorCode = std::get<4>(tupTaskVal);
        if (taskErrorCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
          optJobDesc.setErrorCode(taskErrorCode);
          break;
        }
      }
    } else {
      optJobDesc.setErrorCode(jobErrorCode.value());
    }
  } else if (jobStatus == cpp2::JobStatus::FINISHED) {
    optJobDesc.setErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  auto jobKey = MetaKeyUtils::jobKey(optJobDesc.getSpace(), optJobDesc.getJobId());
  auto jobVal = MetaKeyUtils::jobVal(optJobDesc.getJobType(),
                                     optJobDesc.getParas(),
                                     optJobDesc.getStatus(),
                                     optJobDesc.getStartTime(),
                                     optJobDesc.getStopTime(),
                                     optJobDesc.getErrorCode());

  auto it = runningJobs_.find(jobId);
  // Job has not started yet
  if (it == runningJobs_.end()) {
    // TODO job not existing in runningJobs_ also means leader changed, we handle it later
    cleanJob(jobId);
    return save(jobKey, jobVal);
  }

  // Job has been started
  auto jobExec = it->second.get();
  if (jobStatus == cpp2::JobStatus::STOPPED) {
    auto isRunning = jobExec->isRunning();
    auto code = jobExec->stop();
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
      // meta job is trigger by metad, which runs in async. So we can't clean the job executor
      // here. The cleanJob will be called in runJobInternal.
      if (!jobExec->isMetaJob() && !isRunning) {
        cleanJob(jobId);
        resetSpaceRunning(optJobDesc.getSpace());
      }
      // job has been stopped successfully, so update the job status
      return save(jobKey, jobVal);
    }
    // job could not be stopped, so do not update the job status
    return code;
  } else {
    // If the job is failed or finished, clean and call finish.  We clean the job at first, no
    // matter `finish` return SUCCEEDED or not. Because the job has already come to the end.
    cleanJob(jobId);
    auto rc = save(jobKey, jobVal);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return rc;
    }
    return jobExec->finish(jobStatus == cpp2::JobStatus::FINISHED);
  }
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

// Only the job which execute on storaged will trigger this function. Storage will report to meta
// when the task has been executed. In other words, when storage report the task state, it should
// be one of FINISHED, FAILED or STOPPED.
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
  auto iter = muReportFinish_.find(spaceId);
  if (iter == muReportFinish_.end()) {
    iter = muReportFinish_.emplace(spaceId, std::make_unique<std::mutex>()).first;
  }
  std::lock_guard<std::mutex> lk(*(iter->second));

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

  // the status of task will be set as eithor FINISHED or FAILED in saveTaskStatus
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
    auto code = jobFinished(spaceId, jobId, jobStatus);
    resetSpaceRunning(spaceId);
    return code;
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

nebula::cpp2::ErrorCode JobManager::addJob(JobDescription jobDesc, AdminClient* client) {
  auto mutexIter = muJobFinished_.find(jobDesc.getSpace());
  if (mutexIter == muJobFinished_.end()) {
    mutexIter =
        muJobFinished_.emplace(jobDesc.getSpace(), std::make_unique<std::recursive_mutex>()).first;
  }
  std::lock_guard<std::recursive_mutex> lk(*(mutexIter->second));
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
    inFlightJobs_.emplace(std::move(jobId), std::move(jobDesc));
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
        if (status == cpp2::JobStatus::FAILED) {
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

void JobManager::resetSpaceRunning(GraphSpaceID spaceId) {
  auto iter = muJobFinished_.find(spaceId);
  if (iter == muJobFinished_.end()) {
    iter = muJobFinished_.emplace(spaceId, std::make_unique<std::recursive_mutex>()).first;
  }
  std::lock_guard<std::recursive_mutex> lk(*(iter->second));
  spaceRunningJobs_.insert_or_assign(spaceId, false);
}

nebula::cpp2::ErrorCode JobManager::stopJob(GraphSpaceID spaceId, JobID jobId) {
  LOG(INFO) << folly::sformat("Try to stop job {} in space {}", jobId, spaceId);
  // For meta job
  // 1) spaceRunningJobs_ should not be set when stop, it should be set when
  // runJobInternal is executed
  // 2) JobExecutor cannot be released during execution
  return jobFinished(spaceId, jobId, cpp2::JobStatus::STOPPED);
}

ErrorOr<nebula::cpp2::ErrorCode, uint32_t> JobManager::recoverJob(
    GraphSpaceID spaceId, AdminClient* client, const std::vector<int32_t>& jobIds) {
  auto muIter = muJobFinished_.find(spaceId);
  if (muIter == muJobFinished_.end()) {
    muIter = muJobFinished_.emplace(spaceId, std::make_unique<std::recursive_mutex>()).first;
  }
  std::lock_guard<std::recursive_mutex> lk(*(muIter->second));
  std::set<JobID> jobIdSet(jobIds.begin(), jobIds.end());
  std::map<JobID, JobDescription> allJobs;
  adminClient_ = client;
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
    auto optJobRet = JobDescription::makeJobDescription(iter->key(), iter->val());
    if (!nebula::ok(optJobRet)) {
      LOG(INFO) << "make job description failed, "
                << apache::thrift::util::enumNameSafe(nebula::error(optJobRet));
      return nebula::error(optJobRet);
    }
    auto optJob = nebula::value(optJobRet);
    auto id = optJob.getJobId();
    allJobs.emplace(id, std::move(optJob));
  }
  std::set<JobID> jobsMaybeRecover;
  for (auto& [id, job] : allJobs) {
    auto status = job.getStatus();
    if (status == cpp2::JobStatus::FAILED || status == cpp2::JobStatus::STOPPED) {
      jobsMaybeRecover.emplace(id);
    }
  }
  std::set<JobID>::reverse_iterator lastBalaceJobRecoverIt = jobsMaybeRecover.rend();
  for (auto it = jobsMaybeRecover.rbegin(); it != jobsMaybeRecover.rend(); it++) {
    auto jobType = allJobs[*it].getJobType();
    if (jobType == cpp2::JobType::DATA_BALANCE || jobType == cpp2::JobType::ZONE_BALANCE) {
      lastBalaceJobRecoverIt = it;
      break;
    }
  }
  int32_t recoveredJobNum = 0;
  auto finalyRecover = [&]() -> nebula::cpp2::ErrorCode {
    for (auto& jobId : jobsMaybeRecover) {
      if (!jobIdSet.empty() && !jobIdSet.count(jobId)) {
        continue;
      }
      auto& job = allJobs[jobId];
      JobID jid;
      bool jobExist = checkOnRunningJobExist(spaceId, job.getJobType(), job.getParas(), jid);
      if (!jobExist) {
        job.setStatus(cpp2::JobStatus::QUEUE, true);
        auto jobKey = MetaKeyUtils::jobKey(job.getSpace(), jobId);
        auto jobVal = MetaKeyUtils::jobVal(job.getJobType(),
                                           job.getParas(),
                                           job.getStatus(),
                                           job.getStartTime(),
                                           job.getStopTime(),
                                           job.getErrorCode());
        auto ret = save(jobKey, jobVal);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
          enqueue(spaceId, jobId, JbOp::RECOVER, job.getJobType());
          inFlightJobs_.emplace(jobId, job);
        } else {
          LOG(INFO) << "Add Job Failed";
          if (ret != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            ret = nebula::cpp2::ErrorCode::E_ADD_JOB_FAILURE;
          }
          return ret;
        }
        ++recoveredJobNum;
      }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  nebula::cpp2::ErrorCode rc = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (lastBalaceJobRecoverIt == jobsMaybeRecover.rend()) {
    LOG(INFO) << "no balance jobs, do recover happily";
    rc = finalyRecover();
    if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
      return recoveredJobNum;
    } else {
      return rc;
    }
  }
  JobID lastBalanceJobFinished = -1;
  for (auto it = allJobs.rbegin(); it != allJobs.rend(); it++) {
    auto jobType = it->second.getJobType();
    if ((jobType == cpp2::JobType::DATA_BALANCE || jobType == cpp2::JobType::ZONE_BALANCE) &&
        it->second.getStatus() == cpp2::JobStatus::FINISHED) {
      lastBalanceJobFinished = it->first;
      break;
    }
  }
  for (auto it = jobsMaybeRecover.begin(); it != jobsMaybeRecover.end();) {
    if (*it == *lastBalaceJobRecoverIt) {
      break;
    }
    auto jobType = allJobs[*it].getJobType();
    if (jobType == cpp2::JobType::DATA_BALANCE || jobType == cpp2::JobType::ZONE_BALANCE) {
      if (jobIdSet.empty() || jobIdSet.count(*it)) {
        LOG(INFO) << "can't recover a balance job " << *lastBalaceJobRecoverIt
                  << " when there's a newer balance job " << *lastBalaceJobRecoverIt
                  << " stopped or failed";
      }
      it = jobsMaybeRecover.erase(it);
    } else {
      it++;
    }
  }
  if (*lastBalaceJobRecoverIt < lastBalanceJobFinished) {
    if (jobIdSet.empty() || jobIdSet.count(*lastBalaceJobRecoverIt)) {
      LOG(INFO) << "can't recover a balance job " << *lastBalaceJobRecoverIt
                << " that before a finished balance job " << lastBalanceJobFinished;
    }
    jobsMaybeRecover.erase(*lastBalaceJobRecoverIt);
  }
  rc = finalyRecover();
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    return recoveredJobNum;
  } else {
    return rc;
  }
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
