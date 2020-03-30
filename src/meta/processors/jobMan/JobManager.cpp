/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <boost/stacktrace.hpp>
#include <gtest/gtest.h>
#include <folly/futures/Future.h>
#include <folly/synchronization/Baton.h>
#include <folly/String.h>

#include "base/Base.h"
#include "http/HttpClient.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "meta/MetaServiceUtils.h"
#include "webservice/Common.h"
#include "common/time/WallClock.h"

DEFINE_int32(dispatch_thread_num, 3, "Number of job dispatch http thread");
DEFINE_int32(job_check_intervals, 5000, "job intervals in us");
DEFINE_double(job_expired_secs, 7*24*60*60, "job expired intervals in sec");

using nebula::kvstore::ResultCode;
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
    kvStore_ = store;
    pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
    pool_->start(FLAGS_dispatch_thread_num);

    queue_ = std::make_unique<folly::UMPSCQueue<int32_t, true>>();
    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
    bgThread_->addTask(&JobManager::runJobBackground, this);
    return true;
}

JobManager::~JobManager() {
    shutDown();
}

void JobManager::shutDown() {
    LOG(INFO) << "JobManager::shutDown() begin";
    shutDown_ = true;
    pool_->stop();
    bgThread_->stop();
    bgThread_->wait();
    LOG(INFO) << "JobManager::shutDown() end";
}

void JobManager::runJobBackground() {
    LOG(INFO) << "JobManager::runJobBackground() enter";
    while (!shutDown_) {
        int32_t iJob = 0;
        while (!queue_->try_dequeue(iJob)) {
            if (shutDown_) {
                LOG(INFO) << "[JobManager] detect shutdown called, exit";
                break;
            }
            usleep(FLAGS_job_check_intervals);
        }

        auto jobDesc = JobDescription::loadJobDescription(iJob, kvStore_);
        if (jobDesc == folly::none) {
            LOG(ERROR) << "[JobManager] load a invalid job from queue " << iJob;
            continue;   // leader change or archive happend
        }
        if (!jobDesc->setStatus(cpp2::JobStatus::RUNNING)) {
            LOG(INFO) << "[JobManager] skip job " << iJob;
            continue;
        }
        save(jobDesc->jobKey(), jobDesc->jobVal());

        if (runJobViaThrift(*jobDesc)) {
            jobDesc->setStatus(cpp2::JobStatus::FINISHED);
        } else {
            jobDesc->setStatus(cpp2::JobStatus::FAILED);
        }
        save(jobDesc->jobKey(), jobDesc->jobVal());
    }
    LOG(INFO) << "[JobManager] exit";
}

// tempory name,
// will replace "runJobInternal" when task manager ready
// @return: true if succeed, false if any task failed
bool JobManager::runJobViaThrift(const JobDescription& jobDesc) {
    auto jobExecutor = MetaJobExecutorFactory::createMetaJobExecutor(jobDesc, kvStore_);
    if (jobExecutor == nullptr) {
        LOG(ERROR) << "unreconized job cmd " << static_cast<int>(jobDesc.getCmd());
        return false;
    }
    if (jobDesc.getStatus() == cpp2::JobStatus::STOPPED) {
        jobExecutor->stop();
        return true;
    }
    if (jobDesc.getParas().empty()) {
        return false;
    }

    std::string spaceName = jobDesc.getParas().back();
    int spaceId = getSpaceId(spaceName);
    if (spaceId < 0) {
        return false;
    }

    auto results = jobExecutor->execute();
    if (!nebula::ok(results)) {
        return false;
    }

    size_t taskId = 0;
    bool jobSuccess = true;
    for (auto& hostAndStatus : nebula::value(results)) {
        TaskDescription task(jobDesc.getJobId(), taskId, hostAndStatus.first);
        bool taskSuccess = hostAndStatus.second.ok();
        auto taskStatus = taskSuccess ? cpp2::JobStatus::FINISHED : cpp2::JobStatus::FAILED;
        if (!taskSuccess) {
            jobSuccess = false;
        }
        task.setStatus(taskStatus);
        save(task.taskKey(), task.taskVal());
        ++taskId;
    }
    return jobSuccess;
}

ResultCode JobManager::addJob(const JobDescription& jobDesc) {
    auto rc = save(jobDesc.jobKey(), jobDesc.jobVal());
    if (rc == nebula::kvstore::SUCCEEDED) {
        queue_->enqueue(jobDesc.getJobId());
    }
    return rc;
}

ErrorOr<ResultCode, std::vector<cpp2::JobDesc>>
JobManager::showJobs() {
    std::unique_ptr<kvstore::KVIterator> iter;
    ResultCode rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId,
                                     JobUtil::jobPrefix(), &iter);
    if (rc != nebula::kvstore::SUCCEEDED) {
        return rc;
    }

    int32_t lastExpiredJobId = INT_MIN;
    std::vector<std::string> expiredJobKeys;
    std::vector<cpp2::JobDesc> ret;
    for (; iter->valid(); iter->next()) {
        if (JobDescription::isJobKey(iter->key())) {
            auto optJob = JobDescription::makeJobDescription(iter->key(), iter->val());
            if (optJob == folly::none) {
                continue;
            }
            // skip expired job, default 1 week
            auto jobDesc = optJob->toJobDesc();
            if (isExpiredJob(jobDesc)) {
                lastExpiredJobId = jobDesc.get_id();
                LOG(INFO) << "remove expired job " << lastExpiredJobId;
                expiredJobKeys.emplace_back(iter->key());
                continue;
            }
            ret.emplace_back(jobDesc);
        } else {  // iter-key() is a TaskKey
            TaskDescription task(iter->key(), iter->val());
            if (task.getJobId() == lastExpiredJobId) {
                expiredJobKeys.emplace_back(iter->key());
            }
        }
    }
    removeExpiredJobs(expiredJobKeys);
    std::sort(ret.begin(), ret.end(), [](const auto& a, const auto& b) {
        return a.get_id() > b.get_id();
    });
    return ret;
}

bool JobManager::isExpiredJob(const cpp2::JobDesc& jobDesc) {
    auto jobStart = jobDesc.get_start_time();
    auto duration = std::difftime(nebula::time::WallClock::fastNowInSec(), jobStart);
    return duration > FLAGS_job_expired_secs;
}

void JobManager::removeExpiredJobs(const std::vector<std::string>& expiredJobsAndTasks) {
    folly::Baton<true, std::atomic> baton;
    kvStore_->asyncMultiRemove(kDefaultSpaceId, kDefaultPartId, expiredJobsAndTasks,
        [&](nebula::kvstore::ResultCode code){
            if (code != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "kvstore asyncRemoveRange failed: " << code;
            }
            baton.post();
        });
    baton.wait();
}

ErrorOr<ResultCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>>
JobManager::showJob(int iJob) {
    auto jobKey = JobDescription::makeJobKey(iJob);
    std::unique_ptr<kvstore::KVIterator> iter;
    ResultCode rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobKey, &iter);
    if (rc != nebula::kvstore::SUCCEEDED) {
        return rc;
    }

    if (!iter->valid()) {
        return nebula::kvstore::ERR_KEY_NOT_FOUND;
    }

    std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>> ret;
    for (; iter->valid(); iter->next()) {
        if (JobDescription::isJobKey(iter->key())) {
            auto optJob = JobDescription::makeJobDescription(iter->key(), iter->val());
            if (optJob != folly::none) {
                ret.first = optJob->toJobDesc();
            }
        } else {
            TaskDescription td(iter->key(), iter->val());
            ret.second.emplace_back(td.toTaskDesc());
        }
    }
    return ret;
}

ResultCode JobManager::stopJob(int32_t iJob) {
    auto jobDesc = JobDescription::loadJobDescription(iJob, kvStore_);
    if (jobDesc == folly::none) {
        return nebula::kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }

    jobDesc->setStatus(cpp2::JobStatus::STOPPED);
    return save(jobDesc->jobKey(), jobDesc->jobVal());
}

/*
 * Return: recovered job num.
 * */
ErrorOr<ResultCode, int32_t> JobManager::recoverJob() {
    int32_t recoveredJobNum = 0;
    std::unique_ptr<kvstore::KVIterator> iter;
    ResultCode rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
    if (rc != nebula::kvstore::SUCCEEDED) {
        return rc;
    }
    for (; iter->valid(); iter->next()) {
        if (!JobDescription::isJobKey(iter->key())) {
            continue;
        }
        auto optJob = JobDescription::makeJobDescription(iter->key(), iter->val());
        if (optJob != folly::none) {
            if (optJob->getStatus() == cpp2::JobStatus::QUEUE) {
                queue_->enqueue(optJob->getJobId());
                ++recoveredJobNum;
            }
        }
    }
    return recoveredJobNum;
}

ResultCode JobManager::save(const std::string& k, const std::string& v) {
    std::vector<kvstore::KV> data{std::make_pair(k, v)};
    folly::Baton<true, std::atomic> baton;
    nebula::kvstore::ResultCode rc = nebula::kvstore::SUCCEEDED;
    kvStore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
        [&] (nebula::kvstore::ResultCode code){
        rc = code;
        baton.post();
    });
    baton.wait();
    return rc;
}

int JobManager::getSpaceId(const std::string& name) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(name);
    std::string val;
    auto ret = kvStore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "KVStore error: " << ret;;
        return -1;
    }
    return *reinterpret_cast<const int*>(val.c_str());
}

}  // namespace meta
}  // namespace nebula
