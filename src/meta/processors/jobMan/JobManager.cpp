/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/http/HttpClient.h"
#include "common/webservice/Common.h"
#include "common/time/WallClock.h"
#include <boost/stacktrace.hpp>
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/MetaServiceUtils.h"

DEFINE_int32(job_check_intervals, 5000, "job intervals in us");
DEFINE_double(job_expired_secs, 7 * 24 * 60 * 60, "job expired intervals in sec");

using nebula::kvstore::KVIterator;

namespace nebula {
namespace meta {

JobManager* JobManager::getInstance() {
    static JobManager inst;
    return &inst;
}

bool JobManager::init(nebula::kvstore::KVStore* store) {
    if (store == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lk(statusGuard_);
    if (status_ != JbmgrStatus::NOT_START) {
        return false;
    }
    kvStore_ = store;

    lowPriorityQueue_ = std::make_unique<folly::UMPSCQueue<JobID, true>>();
    highPriorityQueue_ = std::make_unique<folly::UMPSCQueue<JobID, true>>();

    status_ = JbmgrStatus::IDLE;
    bgThread_ = std::thread(&JobManager::scheduleThread, this);
    LOG(INFO) << "JobManager initialized";
    return true;
}

JobManager::~JobManager() {
    shutDown();
}

void JobManager::shutDown() {
    LOG(INFO) << "JobManager::shutDown() begin";
    if (status_ == JbmgrStatus::STOPPED) {  // in case of shutdown more than once
        LOG(INFO) << "JobManager not running, exit";
        return;
    }
    {
        std::lock_guard<std::mutex> lk(statusGuard_);
        status_ = JbmgrStatus::STOPPED;
    }
    bgThread_.join();
    LOG(INFO) << "JobManager::shutDown() end";
}

void JobManager::scheduleThread() {
    LOG(INFO) << "JobManager::runJobBackground() enter";
    while (status_ != JbmgrStatus::STOPPED) {
        int32_t iJob = 0;
        while (status_ == JbmgrStatus::BUSY || !try_dequeue(iJob)) {
            if (status_ == JbmgrStatus::STOPPED) {
                LOG(INFO) << "[JobManager] detect shutdown called, exit";
                break;
            }
            usleep(FLAGS_job_check_intervals);
        }

        auto jobDescRet = JobDescription::loadJobDescription(iJob, kvStore_);
        if (!nebula::ok(jobDescRet)) {
            LOG(ERROR) << "[JobManager] load an invalid job from queue " << iJob;
            continue;   // leader change or archive happend
        }
        auto jobDesc = nebula::value(jobDescRet);
        if (!jobDesc.setStatus(cpp2::JobStatus::RUNNING)) {
            LOG(INFO) << "[JobManager] skip job " << iJob;
            continue;
        }
        save(jobDesc.jobKey(), jobDesc.jobVal());
        {
            std::lock_guard<std::mutex> lk(statusGuard_);
            if (status_ == JbmgrStatus::IDLE) {
                status_ = JbmgrStatus::BUSY;
            }
        }

        if (!runJobInternal(jobDesc)) {
            jobFinished(iJob, cpp2::JobStatus::FAILED);
        }
    }
}

// @return: true if all task dispatched, else false
bool JobManager::runJobInternal(const JobDescription& jobDesc) {
    auto jobExec = MetaJobExecutorFactory::createMetaJobExecutor(jobDesc, kvStore_, adminClient_);
    if (jobExec == nullptr) {
        LOG(ERROR) << "unreconized job cmd "
                   << apache::thrift::util::enumNameSafe(jobDesc.getCmd());
        return false;
    }

    if (jobDesc.getStatus() == cpp2::JobStatus::STOPPED) {
        jobExec->stop();
        return true;
    }

    if (!jobExec->check()) {
        LOG(ERROR) << "Job Executor check failed";
        return false;
    }

    if (jobExec->prepare() != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Job Executor prepare failed";
        return false;
    }

    if (jobExec->execute() != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Job dispatch failed";
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
}

nebula::cpp2::ErrorCode
JobManager::jobFinished(JobID jobId, cpp2::JobStatus jobStatus) {
    LOG(INFO) << folly::sformat("{}, jobId={}, result={}", __func__, jobId,
                                apache::thrift::util::enumNameSafe(jobStatus));
    // normal job finish may race to job stop
    std::lock_guard<std::mutex> lk(muJobFinished_);
    SCOPE_EXIT {
        cleanJob(jobId);
    };
    auto optJobDescRet = JobDescription::loadJobDescription(jobId, kvStore_);
    if (!nebula::ok(optJobDescRet)) {
        LOG(WARNING) << folly::sformat("can't load job, jobId={}", jobId);
        if (jobStatus != cpp2::JobStatus::STOPPED) {
            // there is a rare condition, that when job finished,
            // the job description is deleted(default more than a week)
            // but stop an invalid job should not set status to idle.
            std::lock_guard<std::mutex> statusLk(statusGuard_);
            if (status_ == JbmgrStatus::BUSY) {
                status_ = JbmgrStatus::IDLE;
            }
        }
        return nebula::error(optJobDescRet);
    }

    auto optJobDesc = nebula::value(optJobDescRet);

    if (!optJobDesc.setStatus(jobStatus)) {
        // job already been set as finished, failed or stopped
        return nebula::cpp2::ErrorCode::E_SAVE_JOB_FAILURE;
    }
    {
        std::lock_guard<std::mutex> statusLk(statusGuard_);
        if (status_ == JbmgrStatus::BUSY) {
            status_ = JbmgrStatus::IDLE;
        }
    }
    auto rc = save(optJobDesc.jobKey(), optJobDesc.jobVal());
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return rc;
    }

    auto jobExec =
        MetaJobExecutorFactory::createMetaJobExecutor(optJobDesc, kvStore_, adminClient_);

    if (!jobExec) {
        LOG(WARNING) << folly::sformat("unable to create jobExecutor, jobId={}", jobId);
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
    if (!optJobDesc.getParas().empty()) {
        auto spaceName = optJobDesc.getParas().back();
        auto spaceIdRet = getSpaceId(spaceName);
        if (!nebula::ok(spaceIdRet)) {
            auto retCode = nebula::error(spaceIdRet);
            LOG(INFO) << "Get spaceName "<< spaceName << " failed, error: "
                      << apache::thrift::util::enumNameSafe(retCode);
            return retCode;
        }

        auto spaceId = nebula::value(spaceIdRet);
        if (spaceId == -1) {
            return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
        }
        jobExec->setSpaceId(spaceId);
    }
    if (jobStatus == cpp2::JobStatus::STOPPED) {
        return jobExec->stop();
    } else {
        jobExec->finish(jobStatus == cpp2::JobStatus::FINISHED);
    }

    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode
JobManager::saveTaskStatus(TaskDescription& td,
                           const cpp2::ReportTaskReq& req) {
    auto code = req.get_code();
    auto status = code == nebula::cpp2::ErrorCode::SUCCEEDED ? cpp2::JobStatus::FINISHED :
                                                               cpp2::JobStatus::FAILED;
    td.setStatus(status);

    auto jobId = req.get_job_id();
    auto optJobDescRet = JobDescription::loadJobDescription(jobId, kvStore_);
    if (!nebula::ok(optJobDescRet)) {
        auto retCode = nebula::error(optJobDescRet);
        LOG(WARNING) << "LoadJobDesc failed, jobId " << jobId << " error: "
                     << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    auto optJobDesc = nebula::value(optJobDescRet);
    auto jobExec =
        MetaJobExecutorFactory::createMetaJobExecutor(optJobDesc, kvStore_, adminClient_);

    if (!jobExec) {
        LOG(WARNING) << folly::sformat("createMetaJobExecutor failed(), jobId={}", jobId);
        return nebula::cpp2::ErrorCode::E_TASK_REPORT_OUT_DATE;
    } else {
        if (!optJobDesc.getParas().empty()) {
            auto spaceName = optJobDesc.getParas().back();
            auto spaceIdRet = getSpaceId(spaceName);
            if (!nebula::ok(spaceIdRet)) {
                auto retCode = nebula::error(spaceIdRet);
                LOG(INFO) << "Get spaceName "<< spaceName << " failed, error: "
                          << apache::thrift::util::enumNameSafe(retCode);
                return retCode;
            }

            auto spaceId = nebula::value(spaceIdRet);
            if (spaceId != -1) {
                jobExec->setSpaceId(spaceId);
            }
        }
    }

    auto rcSave = save(td.taskKey(), td.taskVal());
    if (rcSave != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return rcSave;
    }
    return jobExec->saveSpecialTaskStatus(req);
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
    if (status_ == JbmgrStatus::STOPPED || status_ == JbmgrStatus::NOT_START) {
        LOG(INFO) << folly::sformat(
            "report to an in-active job manager, job={}, task={}", jobId, taskId);
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
    // bacause the last task will update the job's status
    // tasks shoule report once a time
    std::lock_guard<std::mutex> lk(muReportFinish_);
    auto tasksRet = getAllTasks(jobId);
    if (!nebula::ok(tasksRet)) {
        return nebula::error(tasksRet);
    }
    auto tasks = nebula::value(tasksRet);
    auto task = std::find_if(tasks.begin(), tasks.end(), [&](auto& it){
        return it.getJobId() == jobId && it.getTaskId() == taskId;
    });
    if (task == tasks.end()) {
        LOG(WARNING) << folly::sformat(
            "report an invalid or outdate task, will ignore this report, job={}, task={}",
            jobId,
            taskId);
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    auto rc = saveTaskStatus(*task, req);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return rc;
    }

    auto allTaskFinished = std::none_of(tasks.begin(), tasks.end(), [](auto& tsk){
        return tsk.status_ == cpp2::JobStatus::RUNNING;
    });

    if (allTaskFinished) {
        auto jobStatus = std::all_of(tasks.begin(), tasks.end(), [](auto& tsk) {
            return tsk.status_ == cpp2::JobStatus::FINISHED;
        }) ? cpp2::JobStatus::FINISHED : cpp2::JobStatus::FAILED;
        return jobFinished(jobId, jobStatus);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::list<TaskDescription>>
JobManager::getAllTasks(JobID jobId) {
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

nebula::cpp2::ErrorCode
JobManager::addJob(const JobDescription& jobDesc, AdminClient* client) {
    auto rc = save(jobDesc.jobKey(), jobDesc.jobVal());
    if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
        auto jobId = jobDesc.getJobId();
        enqueue(jobId, jobDesc.getCmd());
        // Add job to jobMap
        inFlightJobs_.emplace(jobId, jobDesc);
    } else {
        LOG(ERROR) << "Add Job Failed";
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

bool JobManager::try_dequeue(JobID& jobId) {
    if (highPriorityQueue_->try_dequeue(jobId)) {
        return true;
    } else if (lowPriorityQueue_->try_dequeue(jobId)) {
        return true;
    }
    return false;
}

void JobManager::enqueue(const JobID& jobId, const cpp2::AdminCmd& cmd) {
    if (cmd == cpp2::AdminCmd::STATS) {
        highPriorityQueue_->enqueue(jobId);
    } else {
        lowPriorityQueue_->enqueue(jobId);
    }
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::JobDesc>>
JobManager::showJobs() {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId,
                               JobUtil::jobPrefix(), &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Jobs Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
        LOG(ERROR) << "Remove Expired Jobs Failed";
        return retCode;
    }

    std::sort(ret.begin(), ret.end(), [](const auto& a, const auto& b) {
        return a.get_id() > b.get_id();
    });
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
    if (status == cpp2::JobStatus::QUEUE || status == cpp2::JobStatus::RUNNING) {
        return true;
    }
    return false;
}

nebula::cpp2::ErrorCode
JobManager::removeExpiredJobs(std::vector<std::string>&& expiredJobsAndTasks) {
    nebula::cpp2::ErrorCode ret;
    folly::Baton<true, std::atomic> baton;
    kvStore_->asyncMultiRemove(kDefaultSpaceId, kDefaultPartId, std::move(expiredJobsAndTasks),
                               [&](nebula::cpp2::ErrorCode code) {
                                   if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                                       LOG(ERROR) << "kvstore asyncRemoveRange failed: "
                                                  << apache::thrift::util::enumNameSafe(code);
                                   }
                                   ret = code;
                                   baton.post();
                               });
    baton.wait();
    return ret;
}

bool JobManager::checkJobExist(const cpp2::AdminCmd& cmd,
                               const std::vector<std::string>& paras,
                               JobID& iJob) {
    JobDescription jobDesc(0, cmd, paras);
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
JobManager::showJob(JobID iJob) {
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
            ret.first = optJob.toJobDesc();
        } else {
            TaskDescription td(jKey, iter->val());
            ret.second.emplace_back(td.toTaskDesc());
        }
    }
    return ret;
}

nebula::cpp2::ErrorCode JobManager::stopJob(JobID iJob) {
    LOG(INFO) << "try to stop job " << iJob;
    return jobFinished(iJob, cpp2::JobStatus::STOPPED);
}

/*
 * Return: recovered job num.
 * */
ErrorOr<nebula::cpp2::ErrorCode, JobID> JobManager::recoverJob() {
    int32_t recoveredJobNum = 0;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find jobs, error: " << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    for (; iter->valid(); iter->next()) {
        if (!JobDescription::isJobKey(iter->key())) {
            continue;
        }
        auto optJobRet = JobDescription::makeJobDescription(iter->key(), iter->val());
        if (nebula::ok(optJobRet)) {
            auto optJob = nebula::value(optJobRet);
            if (optJob.getStatus() == cpp2::JobStatus::QUEUE) {
                // Check if the job exists
                JobID jId = 0;
                auto jobExist = checkJobExist(optJob.getCmd(), optJob.getParas(), jId);

                if (!jobExist) {
                    auto jobId = optJob.getJobId();
                    enqueue(jobId, optJob.getCmd());
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
    kvStore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                            [&] (nebula::cpp2::ErrorCode code) {
                                rc = code;
                                baton.post();
                            });
    baton.wait();
    return rc;
}

ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> JobManager::getSpaceId(const std::string& name) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(name);
    std::string val;
    auto retCode = kvStore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &val);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            retCode = nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        }
        LOG(ERROR) << "KVStore error: " << apache::thrift::util::enumNameSafe(retCode);;
        return retCode;
    }
    return *reinterpret_cast<const GraphSpaceID*>(val.c_str());
}

ErrorOr<nebula::cpp2::ErrorCode, bool> JobManager::checkIndexJobRuning() {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Jobs Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
            auto cmd = jobDesc.getCmd();
            if (cmd == cpp2::AdminCmd::REBUILD_TAG_INDEX ||
                cmd == cpp2::AdminCmd::REBUILD_EDGE_INDEX) {
                return true;
            }
        }
    }

    return false;
}

}  // namespace meta
}  // namespace nebula
