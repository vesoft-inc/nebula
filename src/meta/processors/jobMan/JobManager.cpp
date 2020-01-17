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
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/MetaServiceUtils.h"
#include "webservice/Common.h"

DEFINE_int32(dispatch_thread_num, 3, "Number of job dispatch http thread");
DEFINE_int32(job_check_intervals, 5000, "job intervals in us");

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
        if (!jobDesc->setStatus(JobStatus::Status::RUNNING)) {
            LOG(INFO) << "[JobManager] skip job " << iJob;
            continue;
        }
        save(jobDesc->jobKey(), jobDesc->jobVal());

        if (runJobInternal(*jobDesc)) {
            jobDesc->setStatus(JobStatus::Status::FINISHED);
        } else {
            jobDesc->setStatus(JobStatus::Status::FAILED);
        }
        save(jobDesc->jobKey(), jobDesc->jobVal());
    }
    LOG(INFO) << "[JobManager] exit";
}

bool JobManager::runJobInternal(const JobDescription& jobDesc) {
    std::unique_ptr<kvstore::KVIterator> iter;
    std::string spaceName = jobDesc.getParas().back();
    int spaceId = getSpaceId(spaceName);
    if (spaceId == -1) {
        return false;
    }
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    auto ret = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return false;
    }

    std::string op = jobDesc.getType();
    std::unordered_set<std::string> storageIPs;
    while (iter->valid()) {
        for (auto &host : MetaServiceUtils::parsePartVal(iter->val())) {
            auto storageIP = network::NetworkUtils::intToIPv4(host.get_ip());
            storageIPs.insert(storageIP);
        }
        iter->next();
    }

    int32_t iJob = jobDesc.getJobId();
    std::vector<folly::SemiFuture<bool>> futures;
    size_t iTask = 0;
    for (auto& storageIP : storageIPs) {
        auto dispatcher = [=]() {
            static const char *tmp = "http://%s:%d/admin?op=%s&space=%s";
            auto url = folly::stringPrintf(tmp, storageIP.c_str(),
                                           FLAGS_ws_storage_http_port,
                                           op.c_str(),
                                           spaceName.c_str());
            LOG(INFO) << "make admin url: " << url << ", iTask=" << iTask;
            TaskDescription taskDesc(iJob, iTask, storageIP);
            save(taskDesc.taskKey(), taskDesc.taskVal());

            auto httpResult = nebula::http::HttpClient::get(url, "-GSs");
            bool succeed = httpResult.ok() && httpResult.value() == "ok";

            if (succeed) {
                taskDesc.setStatus(JobStatus::Status::FINISHED);
            } else {
                taskDesc.setStatus(JobStatus::Status::FAILED);
            }

            save(taskDesc.taskKey(), taskDesc.taskVal());
            return succeed;
        };
        ++iTask;
        auto future = pool_->addTask(dispatcher);
        futures.push_back(std::move(future));
    }

    bool successfully{true};
    folly::collectAll(std::move(futures))
        .thenValue([&](const std::vector<folly::Try<bool>>& tries) {
            for (const auto& t : tries) {
                if (t.hasException()) {
                    LOG(ERROR) << "admin Failed: " << t.exception();
                    successfully = false;
                    break;
                }
            }
        }).thenError([&](auto&& e){
            LOG(ERROR) << "admin Failed: " << e.what();
            successfully = false;
        }).wait();
    LOG(INFO) << "admin tasks have finished";
    return successfully;
}

StatusOr<JobDescription>
JobManager::buildJobDescription(const std::vector<std::string>& args) {
    using retType = StatusOr<JobDescription>;
    auto command = args[0];
    std::vector<std::string> paras(args.begin() + 1, args.end());
    auto errOrJobId = reserveJobId();
    if (!errOrJobId.ok()) {
        return retType(errOrJobId.status());
    }
    return retType(JobDescription(errOrJobId.value(), command, paras));
}

int32_t JobManager::addJob(const JobDescription& jobDesc) {
    save(jobDesc.jobKey(), jobDesc.jobVal());
    queue_->enqueue(jobDesc.getJobId());
    return jobDesc.getJobId();
}

StatusOr<std::vector<cpp2::JobDetails>>
JobManager::showJobs() {
    using TReturn = StatusOr<std::vector<cpp2::JobDetails>>;
    TReturn ret;
    std::unique_ptr<kvstore::KVIterator> iter;
    ResultCode rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId,
                                     JobUtil::jobPrefix(), &iter);
    if (rc != nebula::kvstore::SUCCEEDED) {
        ret = Status::Error("kvstore err code = %d", rc);
        return ret;
    }
    ret = std::vector<cpp2::JobDetails>{};

    for (; iter->valid(); iter->next()) {
        if (JobDescription::isJobKey(iter->key())) {
            auto optJob = JobDescription::makeJobDescription(iter->key(), iter->val());
            if (optJob != folly::none) {
                ret.value().emplace_back(optJob->toJobDetails());
            }
        }
    }
    return ret;
}

StatusOr<std::pair<cpp2::JobDetails, std::vector<cpp2::TaskDetails>>>
JobManager::showJob(int iJob) {
    using TReturn = StatusOr<std::pair<cpp2::JobDetails, std::vector<cpp2::TaskDetails>>>;
    std::pair<cpp2::JobDetails, std::vector<cpp2::TaskDetails>> ret;
    auto jobKey = JobDescription::makeJobKey(iJob);
    std::unique_ptr<kvstore::KVIterator> iter;
    ResultCode rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, jobKey, &iter);
    if (rc != nebula::kvstore::SUCCEEDED) {
        return TReturn(Status::Error("kvstore err code = %d", rc));
    }

    for (; iter->valid(); iter->next()) {
        if (JobDescription::isJobKey(iter->key())) {
            auto optJob = JobDescription::makeJobDescription(iter->key(), iter->val());
            if (optJob != folly::none) {
                ret.first = optJob->toJobDetails();
            }
        } else {
            TaskDescription td(iter->key(), iter->val());
            ret.second.emplace_back(td.toTaskDetails());
        }
    }
    return ret;
}

nebula::Status JobManager::stopJob(int32_t iJob) {
    auto jobDesc = JobDescription::loadJobDescription(iJob, kvStore_);
    if (jobDesc == folly::none) {
        return Status::Error("kvstore err %d", nebula::kvstore::ResultCode::ERR_KEY_NOT_FOUND);
    }

    jobDesc->setStatus(JobStatus::Status::STOPPED);
    save(jobDesc->jobKey(), jobDesc->jobVal());

    return Status::OK();
}

// return pair(backupJobNum, backupTaskNum)
std::pair<int, int> JobManager::backupJob(int iBegin, int iEnd) {
    int backupJobNum = 0;
    int backupTaskNum = 0;

    std::string keyBegin = JobDescription::makeJobKey(iBegin);
    std::string keyEnd = JobDescription::makeJobKey(iEnd+1);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto rc = kvStore_->range(kDefaultSpaceId, kDefaultPartId, keyBegin, keyEnd, &iter);
    if (rc != nebula::kvstore::SUCCEEDED) {
        return std::make_pair(0, 0);
    }

    while (iter->valid()) {
        if (JobDescription::isJobKey(iter->key())) {
            auto optJob = JobDescription::makeJobDescription(iter->key(), iter->val());
            if (optJob != folly::none) {
                save(optJob->archiveKey(), iter->val().str());
                ++backupJobNum;
            }
        } else {
            TaskDescription td(iter->key(), iter->val());
            save(td.archiveKey(), iter->val().str());
            ++backupTaskNum;
        }
        iter->next();
    }

    folly::Baton<true, std::atomic> baton;
    kvStore_->asyncRemoveRange(kDefaultSpaceId, kDefaultPartId, keyBegin, keyEnd,
        [&](nebula::kvstore::ResultCode code){
            if (code != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "kvstore asyncRemoveRange failed: " << code;
            }
            baton.post();
        });
    baton.wait();
    return std::make_pair(backupJobNum, backupTaskNum);
}

/*
 * Return: recovered job num.
 * */
int32_t JobManager::recoverJob() {
    int32_t recoveredJobNum = 0;
    std::unique_ptr<kvstore::KVIterator> iter;
    kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
    for (; iter->valid(); iter->next()) {
        if (!JobDescription::isJobKey(iter->key())) { continue; }
        auto optJob = JobDescription::makeJobDescription(iter->key(), iter->val());
        if (optJob == folly::none) { continue; }
        if (optJob->getStatus() == JobStatus::Status::QUEUE) {
            queue_->enqueue(optJob->getJobId());
            ++recoveredJobNum;
        }
    }
    return recoveredJobNum;
}

StatusOr<int32_t> JobManager::reserveJobId() {
    folly::SharedMutex::WriteHolder holder(LockUtils::idLock());

    auto currId = JobUtil::currJobKey();
    std::string val;
    ResultCode rc = kvStore_->get(kDefaultPartId, kDefaultSpaceId, currId, &val);
    if (rc == nebula::kvstore::ERR_KEY_NOT_FOUND) {
        val = std::to_string(0);    // assume there is a val 0
    } else if (rc != nebula::kvstore::SUCCEEDED) {
        std::string msg = folly::stringPrintf("kvStore error %d", rc);
        LOG(ERROR) << msg;
        return StatusOr<int32_t>(Status::Error(msg));
    }

    int32_t oldId = atoi(val.c_str());
    int32_t newId = oldId + 1;

    rc = save(currId, std::to_string(newId));
    if (rc != nebula::kvstore::SUCCEEDED) {
        std::string msg = folly::stringPrintf("kvStore error %d", rc);
        LOG(ERROR) << msg;
        return Status::Error(msg);
    }

    return newId;
}

nebula::kvstore::ResultCode JobManager::save(const std::string& k, const std::string& v) {
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
