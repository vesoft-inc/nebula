/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <boost/stacktrace.hpp>
#include <gtest/gtest.h>
#include <folly/futures/Future.h>
#include <folly/synchronization/Baton.h>
#include <folly/String.h>

#include "base/Base.h"
#include "http/HttpClient.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"
#include "meta/TaskDescription.h"
#include "meta/JobStatus.h"
#include "meta/KVJobManager.h"
#include "meta/MetaServiceUtils.h"
#include "webservice/Common.h"

using nebula::kvstore::ResultCode;
using nebula::kvstore::KVIterator;
using ErrOrString = nebula::ErrorOr<nebula::kvstore::ResultCode, std::string>;
using ErrOrInt = nebula::ErrorOr<nebula::kvstore::ResultCode, int>;

namespace nebula {
namespace meta {

KVJobManager::KVJobManager() {
}

KVJobManager* KVJobManager::getInstance() {
    static KVJobManager inst;
    return &inst;
}

bool KVJobManager::init(nebula::kvstore::KVStore* store,
                        nebula::thread::GenericThreadPool *pool) {
    if (!store || !pool) return false;
    KVJobManager* inst = KVJobManager::getInstance();
    inst->kvStore_ = store;
    inst->pool_ = pool;

    kCurrId = kJobPrefix + "__id";
    kJobKey = kJobPrefix + "_";
    kJobArchive = kJobPrefix + "archive_";

    executor_ = std::thread(&KVJobManager::runBackground, this);
    executor_.detach();

    return true;
}

ErrOrString KVJobManager::runJob(const std::string& op,
                                std::vector<std::string>& paras) {
    if (op == "add_job") {
        return addJobWrapper(paras);
    } else if (op == "show_jobs") {
        return showJobsWrapper();
    } else if (op == "show_job") {
        return showJobWrapper(paras);
    } else if (op == "stop_job") {
        return stopJobWrapper(paras);
    } else if (op == "backup_job") {
        return stopJobWrapper(paras);
    } else if (op == "recover_job") {
        return stopJobWrapper(paras);
    } else {
        return nebula::kvstore::ERR_INVALID_ARGUMENT;
    }
}

ErrOrString KVJobManager::addJobWrapper(std::vector<std::string>& paras) {
    if (paras.size() < 2) {
        return nebula::kvstore::ERR_INVALID_ARGUMENT;
    }
    auto ret = addJob(paras[0], paras[1]);
    if (!ok(ret)) {
        return error(ret);
    }
    return std::to_string(value(ret));
}

ErrOrString KVJobManager::showJobsWrapper() {
    auto jobDescriptions = showJobs();
    std::string ret{""};
    if (jobDescriptions.empty()) {
        return ret;
    }
    for (auto& job : jobDescriptions) {
        ret.append(job).append(kRowDelimeter);
    }
    return ret.substr(0, ret.length() - 1);
}

ErrOrString KVJobManager::showJobWrapper(std::vector<std::string>& paras) {
    if (paras.empty()) {
        return nebula::kvstore::ERR_INVALID_ARGUMENT;
    }
    std::string ret{""};
    int iJob = atoi(paras[0].c_str());
    auto tasks = showJob(iJob);
    if (tasks.empty()) {
        return nebula::kvstore::ERR_KEY_NOT_FOUND;
    }
    for (auto& t : tasks) {
        ret.append(t).append(kRowDelimeter);
    }
    return ret.substr(0, ret.length() - 1);
}

ErrOrString KVJobManager::stopJobWrapper(std::vector<std::string>& paras) {
    if (paras.empty()) {
        return nebula::kvstore::ERR_INVALID_ARGUMENT;
    }
    std::string ret{""};
    int job_id = atoi(paras[0].c_str());
    auto ans = stopJob(job_id);
    if (ans) {
        ret = "job " + std::to_string(job_id) + " stopped";
    } else {
        ret = "invalid job id " + std::to_string(job_id);
    }
    return ret;
}

ErrOrString KVJobManager::backupJobWrapper(std::vector<std::string>& paras) {
    for (auto& str : paras) {
        LOG(INFO) << str;
    }

    int iStart = atoi(paras[0].c_str());
    int iStop = atoi(paras[1].c_str());
    auto ret = backupJob(iStart, iStop);
    std::string msg = folly::stringPrintf("backup %d job%s %d task%s",
                                           ret.first,
                                           ret.first > 0 ? "s" : "",
                                           ret.second,
                                           ret.second > 0 ? "s" : "");
    return msg;
}

ErrOrString KVJobManager::recoverJobWrapper(std::vector<std::string>& paras) {
    int iJob = atoi(paras[0].c_str());

    auto ret = recoverJob(iJob);
    if (ok(ret)) {
        return std::to_string(value(ret));
    }
    return nebula::kvstore::ERR_INVALID_ARGUMENT;
}

void KVJobManager::runBackground() {
    for (;;) {
        auto spJob = queue_.take();
        if (!spJob) break;  // which means stop called
        ResultCode rc = setJobStatus(*spJob, -1, JobStatus::running);
        if (rc != nebula::kvstore::SUCCEEDED) {
            continue;
        }
        auto success = runInternal(*spJob);
        std::string status = success ? JobStatus::finished : JobStatus::failed;

        setJobStatus(*spJob, -1, status);
    }
}

bool KVJobManager::runInternal(int iJob) {
    LOG(INFO) << "admin run job = " << iJob;
    std::string jobKey = encodeJobKey(iJob);
    std::string jobVal;
    ResultCode rc = kvStore_->get(kSpace, kPart, jobKey, &jobVal);
    if (rc != nebula::kvstore::SUCCEEDED) {
        LOG(ERROR) << "Unable to get job :" << iJob;
        return false;
    }

    std::unique_ptr<JobDescription> spJobDesc(new JobDescription(jobVal));
    if (!spJobDesc) {
        LOG(ERROR) << "Invalid job record:" << jobVal;
        return false;
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    std::string spaceName = spJobDesc->para;
    int spaceId = getSpaceId(spaceName);
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    static const GraphSpaceID metaSpaceId = 0;
    static const PartitionID  metaPartId = 0;
    auto ret = kvStore_->prefix(metaSpaceId, metaPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return false;
    }

    std::string op = spJobDesc->type;
    std::vector<std::string> storageIPs;
    while (iter->valid()) {
        for (auto &host : MetaServiceUtils::parsePartVal(iter->val())) {
            auto storageIP = network::NetworkUtils::intToIPv4(host.get_ip());
            if (std::find(storageIPs.begin(), storageIPs.end(), storageIP) == storageIPs.end()) {
                storageIPs.emplace_back(storageIP);
            }
        }
        iter->next();
    }

    std::vector<folly::SemiFuture<bool>> futures;
    for (size_t iTask = 0; iTask != storageIPs.size(); ++iTask) {
        auto dispatcher = [=]() {
            auto& storageIP = storageIPs[iTask];
            static const char *tmp = "http://%s:%d/admin?op=%s&space=%s";
            auto url = folly::stringPrintf(tmp, storageIP.c_str(),
                                           FLAGS_ws_storage_http_port,
                                           op.c_str(),
                                           spaceName.c_str());
            LOG(INFO) << "make admin url: " << url << ", iTask=" << iTask;
            setTaskStatus(iJob, iTask, storageIP, JobStatus::running);
            auto AdminResult = nebula::http::HttpClient::get(url, "-GSs");
            bool ans = AdminResult.ok() && AdminResult.value() == "ok";

            std::string status = ans ? JobStatus::finished : JobStatus::failed;
            if (!ans) {
                LOG(INFO) << "AdminResult.ok()=" << AdminResult.ok()
                       << ", AdminResult.value()=" << AdminResult.value();
            }
            setTaskStatus(iJob, iTask, storageIP, status);
            return ans;
        };
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
            if (!t.value()) {
                successfully = false;
                break;
            }
        }
    }).wait();
    LOG(INFO) << "admin tasks have finished";
    return successfully;
}

KVJobManager::~KVJobManager() {
    shutDown();
}

void KVJobManager::shutDown() {
    queue_.stop();
}

/*
 * add a logic job.
 *      Will add one item in kvstore
 *      key is __job_mgr + job id (without task), e.g. __job_mgr_d2019
 *      value is a job description(op|para|status|start time|stop time).
 *
 * kvstore example
 * __job_mgr_d2019   | compact my_space2 ...       (logic job)
 * __job_mgr_d2019a0 | running   | 192.168.8.5 ... (1st task)
 * __job_mgr_d2019a1 | finished  | 192.168.8.6 ... (2nd task)
 *
 * Return:
 *      int if succeed
 * */
ErrOrInt KVJobManager::addJob(const std::string& type, const std::string& para) {
    LOG(INFO) << folly::stringPrintf("[%s] type=%s, para=%s\n", __func__,
                                     type.c_str(), para.c_str());
    auto iJob = reserveJobId();
    if (!ok(iJob)) {
        return error(iJob);
    }
    auto job_key = encodeJobKey(value(iJob));
    LOG(INFO) << folly::stringPrintf("[%s] newJobId=%d\n", __func__, value(iJob));

    JobDescription jd;
    jd.type = type;
    jd.para = para;
    jd.status = "queue";
    jd.startTime = timeNow();

    auto rc = setSingleVal(job_key, jd.toString());
    if (rc != nebula::kvstore::SUCCEEDED) {
        LOG(ERROR) << folly::stringPrintf("[%s] failed, error code = %d\n", __func__, rc);
        return rc;
    }

    queue_.put(value(iJob));
    return iJob;
}

std::vector<std::string> KVJobManager::showJobs() {
    std::vector<std::string> ret;

    std::unique_ptr<kvstore::KVIterator> iter;
    kvStore_->prefix(kSpace, kPart, kJobKey, &iter);

    for (; iter->valid(); iter->next()) {
        auto job_task = decodeJobKey(iter->key().str());
        if (job_task.second >= 0) {     // skip tasks
            continue;
        }
        LOG(INFO) << "iter->val() " << iter->val().str();
        int iJob = job_task.first;
        auto jd = getJobDescription(iJob);
        jd->para = jd->type + " " + jd->para;
        jd->type = std::to_string(iJob);
        ret.emplace_back(jd->toString());
        LOG(INFO) << "emplace_back " << ret.back();
    }
    return ret;
}

std::vector<std::string> KVJobManager::showJob(int iJob) {
    LOG(INFO) << "iJob=" << iJob;
    std::vector<std::string> ret;
    auto key = encodeJobKey(iJob);

    std::unique_ptr<kvstore::KVIterator> iter;
    ResultCode rc = kvStore_->prefix(kSpace, kPart, key, &iter);
    if (rc != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    // first add job description
    if (iter && iter->valid()) {
        JobDescription jd(iter->val().str());
        jd.para = jd.type + " " + jd.para;
        jd.type = std::to_string(iJob);
        ret.emplace_back(jd.toString());
        iter->next();
    }

    // add all task description
    int iTask = 0;
    while (iter && iter->valid()) {
        TaskDescription td(iter->val().str());
        std::stringstream oss;
        oss << iJob << "-" << iTask << delim_
            << td.dest << delim_
            << td.status << delim_
            << td.startTime << delim_
            << td.stopTime;
        ret.emplace_back(oss.str());
        iter->next();
        iTask++;
    }

    return ret;
}

// Description:
//      1.1 remove iJob from class member queue
//      1.2 set job status to 'stopped' in kvstore
// Return:
//      1 if 1.1 & 1.2 succeed
//      0 if iJob not exist in queue
//      0 if key not exist in kvstore
int KVJobManager::stopJob(int iJob) {
    LOG(INFO) << " iJob = " << iJob;

    // 1. remove job id from queue
    if (!queue_.remove(iJob)) {
        return 0;
    }

    // 2. set job status to 'stopped' in kvstore
    ResultCode rc = setJobStatus(iJob, -1, JobStatus::stopped);
    return rc == nebula::kvstore::SUCCEEDED;
}

// Return
//      pair(task, job)
std::pair<int, int>
KVJobManager::backupJob(int iBegin, int iEnd) {
    LOG(INFO) << "begin=" << iBegin << ", end=" << iEnd;
    int iJobBackup = 0;
    int iTaskBackup = 0;

    std::string kBegin = encodeJobKey(iBegin);
    std::string kEnd = encodeJobKey(iEnd+1);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto rc = kvStore_->range(kSpace, kPart, kBegin, kEnd, &iter);
    if (rc != nebula::kvstore::SUCCEEDED) return std::make_pair(0, 0);

    std::vector<kvstore::KV> data;
    while (iter->valid()) {
        if (data.size() < 500) {
            data.emplace_back(std::make_pair(kJobArchive + iter->key().str(), iter->val().str()));
        } else {
            folly::Baton<true, std::atomic> baton;
            kvStore_->asyncMultiPut(kSpace, kPart, std::move(data),
                [&] (nebula::kvstore::ResultCode code){
                LOG(INFO) << code;
                baton.post();
            });
            baton.wait();
            data.clear();
        }
        auto job_task = decodeJobKey(iter->key().str());
        if (job_task.second == -1) {
            ++iJobBackup;
        } else {
            ++iTaskBackup;
        }

        iter->next();
    }
    if (!data.empty()) {
        folly::Baton<true, std::atomic> baton;
        kvStore_->asyncMultiPut(kSpace, kPart, std::move(data),
            [&] (nebula::kvstore::ResultCode code){
            assert(code == kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
        baton.wait();
    }

    folly::Baton<true, std::atomic> baton;
    kvStore_->asyncRemoveRange(kSpace, kPart, kBegin, kEnd,
        [&](nebula::kvstore::ResultCode code){
            assert(code == kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
    baton.wait();
    return std::make_pair(iJobBackup, iTaskBackup);
}

// Return
//      new Job Id if succeed
//      -1 iff no job desc in kv store
ErrOrInt KVJobManager::recoverJob(int iJob) {
    LOG(INFO) << "iJob=" << iJob;
    auto jobDesc = getJobDescription(iJob);
    if (!jobDesc) {
        return 0;
    }

    return addJob(jobDesc->type, jobDesc->para);
}

ErrOrInt KVJobManager::reserveJobId() {
    // todo distributed lock
    std::string strId;
    ResultCode rc = kvStore_->get(kSpace, kPart, kCurrId, &strId);
    int ret = -1;
    if (rc == nebula::kvstore::SUCCEEDED) {
        ret = atoi(strId.c_str());
    }
    ++ret;

    rc = setSingleVal(kCurrId, std::to_string(ret));
    if (rc != nebula::kvstore::SUCCEEDED) {
        return rc;
    }

    return ret;
}

nebula::kvstore::ResultCode KVJobManager::setSingleVal(const std::string& k, const std::string& v) {
    std::vector<kvstore::KV> data{std::make_pair(k, v)};
    folly::Baton<true, std::atomic> baton;
    nebula::kvstore::ResultCode rc;
    kvStore_->asyncMultiPut(kSpace, kPart, std::move(data),
        [&] (nebula::kvstore::ResultCode code){
        rc = code;
        baton.post();
    });
    baton.wait();
    return rc;
}

/* Return
 *  Succeed
 *      SUCCEEDED
 *  Failed
 *      ERR_KEY_NOT_FOUND      if no specific job/task in kvstore
 *      ERR_INVALID_ARGUMENT   if current status already later than status
 * */
ResultCode KVJobManager::setJobStatus(int iJob, int iTask, const std::string& status) {
    LOG(INFO) << "iJob=" << iJob << ", iTask=" << iTask << ", status=" << status;
    auto jobKey = encodeJobKey(iJob, iTask);

    auto jd = getJobDescription(iJob, iTask);
    if (!jd) {
        return nebula::kvstore::ERR_KEY_NOT_FOUND;
    }

    if (JobStatus::laterThan(jd->status, status)) {
        return nebula::kvstore::ERR_INVALID_ARGUMENT;
    }

    jd->status = status;

    if (status == JobStatus::running) {
        jd->startTime = timeNow();
    }
    if (JobStatus::laterThan(status, JobStatus::running)) {
        jd->stopTime = timeNow();
    }

    setSingleVal(jobKey, jd->toString());
    return nebula::kvstore::SUCCEEDED;
}

// val shoule be : dest|status
// may failed if
//      1. try to set task status running but job already stopped
//      2. try to set finished/failed/stopped, but there is no target key
ResultCode KVJobManager::setTaskStatus(int iJob, int iTask, const std::string& sDest,
                                       const std::string& newStatus) {
    LOG(INFO) << "iJob=" << iJob << ", iTask=" << iTask << ", dest=" << sDest
              << ", newStatus=" << newStatus;
    auto key = encodeJobKey(iJob, iTask);

    TaskDescription td;
    std::string val;
    ResultCode rc = kvStore_->get(kSpace, kPart, key, &val);
    if (rc == nebula::kvstore::SUCCEEDED) {
        td = TaskDescription(val);
    } else if (rc == nebula::kvstore::ERR_KEY_NOT_FOUND) {
        // if newStatus later than running, there must be a key
        if (JobStatus::laterThan(newStatus, JobStatus::running)) {
            LOG(INFO) << "ERR_INVALID_ARGUMENT";
            return nebula::kvstore::ERR_INVALID_ARGUMENT;
        }
    } else {
        LOG(INFO) << "rc";
        return rc;
    }

    td.status = newStatus;
    if (JobStatus::laterThan(td.status, newStatus)) {
        LOG(INFO) << "ERR_INVALID_ARGUMENT oldStatus=" << td.status << ", newStatus=" << newStatus;
        return nebula::kvstore::ERR_INVALID_ARGUMENT;
    }

    td.dest = sDest;
    if (td.status == JobStatus::running) {
        td.startTime = timeNow();
    }

    if (JobStatus::laterThan(td.status, JobStatus::running)) {
        td.stopTime = timeNow();
    }

    setSingleVal(key, td.toString());
    return nebula::kvstore::SUCCEEDED;
}

std::shared_ptr<JobDescription> KVJobManager::getJobDescription(int iJob, int iTask) {
    std::shared_ptr<JobDescription> ret;
    auto key = encodeJobKey(iJob, iTask);
    std::string val;
    ResultCode rc = kvStore_->get(kSpace, kPart, key, &val);
    if (rc != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    ret.reset(new JobDescription(val));
    return ret;
}


std::string KVJobManager::encodeLenVal(const std::string& src) {
    char c = 'a' + src.length() - 1;
    return std::string(1, c) + src;
}

std::pair<int, int> KVJobManager::decodeLenVal(const std::string& len_val) {
    if (len_val.length() < 0) return std::make_pair(-1, -1);
    if (len_val.length() == 0) return std::make_pair(0, -1);

    int len = len_val[0] - 'a' + 1;
    int val = atoi(len_val.substr(1, len).c_str());
    return std::make_pair(len, val);
}

std::string KVJobManager::encodeJobKey(int job, int task) {
    std::string key;
    key.reserve(30);
    key.append(kJobKey);
    key.append(encodeLenVal(std::to_string(job)));
    if (task >= 0) key.append(encodeLenVal(std::to_string(task)));
    return key;
}

std::pair<int, int> KVJobManager::decodeJobKey(const std::string& key) {
    auto n = key.length();
    int job = -1;
    int task = -1;

    do {
        auto start = kJobKey.length();
        if (start >= n) break;

        auto job_task = key.substr(start);

        auto len_job = decodeLenVal(job_task);
        job = len_job.second;
        start += len_job.first;
        if (start >= n) break;
        auto len_task = decodeLenVal(key.substr(start+1));
        task = len_task.second;
    } while (0);

    return std::make_pair(job, task);
}

int KVJobManager::getSpaceId(const std::string& name) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(name);
    std::string val;
    auto ret = kvStore_->get(0, 0, indexKey, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        return *reinterpret_cast<const int*>(val.c_str());
    }
    return -1;
}

std::string KVJobManager::timeNow() {
    std::time_t tm = std::time(nullptr);
    char mbstr[50];
    int len = std::strftime(mbstr, sizeof(mbstr), "%x %X", std::localtime(&tm));
    std::string ret;
    if (len) {
        ret = std::string(&mbstr[0], len);
    }
    return ret;
}

}  // namespace meta
}  // namespace nebula
