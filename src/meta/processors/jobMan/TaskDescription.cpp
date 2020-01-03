/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "folly/String.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/processors/jobMan/JobUtils.h"

namespace nebula {
namespace meta {

TaskDescription::TaskDescription(int32_t iJob,
                                 int32_t iTask,
                                 const std::string& dest)
                                 : iJob_(iJob),
                                   iTask_(iTask),
                                   dest_(dest),
                                   status_(JobStatus::Status::RUNNING),
                                   startTime_(std::time(nullptr)),
                                   stopTime_(folly::none) {}


/*
 * int32_t                         iJob_;
 * int32_t                         iTask_;
 * std::string                     dest_;
 * JobStatus::Status               status_;
 * std::time_t                     startTime_;
 * std::time_t                     stopTime_;
 * */
TaskDescription::TaskDescription(const folly::StringPiece& key,
                                 const folly::StringPiece& val) {
    auto tupKey = parseKey(key);
    iJob_ = std::get<0>(tupKey);
    iTask_ = std::get<1>(tupKey);

    auto tupVal = parseVal(val);
    dest_ = std::get<0>(tupVal);
    status_ = std::get<1>(tupVal);
    startTime_ = std::get<2>(tupVal);
    stopTime_ = std::get<3>(tupVal);
}

std::string TaskDescription::taskKey() {
    std::string str;
    str.reserve(32);
    str.append(reinterpret_cast<const char*>(JobUtil::jobPrefix().data()),
                                             JobUtil::jobPrefix().size());
    str.append(reinterpret_cast<const char*>(&iJob_), sizeof(iJob_));
    str.append(reinterpret_cast<const char*>(&iTask_), sizeof(iTask_));
    return str;
}

std::tuple<int32_t, int32_t>
TaskDescription::parseKey(const folly::StringPiece& rawKey) {
    auto offset = JobUtil::jobPrefix().size();
    int32_t iJob =  *reinterpret_cast<const int32_t*>(rawKey.begin() + offset);
    offset += sizeof(int32_t);
    int32_t iTask = *reinterpret_cast<const int32_t*>(rawKey.begin() + offset);
    return std::make_tuple(iJob, iTask);
}

std::string TaskDescription::archiveKey() {
    std::string str;
    str.reserve(32);
    str.append(reinterpret_cast<const char*>(JobUtil::archivePrefix().data()),
                                             JobUtil::archivePrefix().size());
    str.append(reinterpret_cast<const char*>(&iJob_), sizeof(iJob_));
    str.append(reinterpret_cast<const char*>(&iTask_), sizeof(iTask_));
    return str;
}

std::string TaskDescription::taskVal() {
    std::string str;
    str.reserve(128);
    auto destLen = dest_.length();
    str.append(reinterpret_cast<const char*>(&destLen), sizeof(destLen));
    str.append(reinterpret_cast<const char*>(&dest_[0]), dest_.length());
    str.append(reinterpret_cast<const char*>(&status_), sizeof(JobStatus::Status));
    str.append(reinterpret_cast<const char*>(&startTime_), sizeof(folly::Optional<std::time_t>));
    str.append(reinterpret_cast<const char*>(&stopTime_), sizeof(folly::Optional<std::time_t>));
    return str;
}

/*
 *  std::string                     dest_;
 *  JobStatus::Status               status_;
 *  std::time_t                     startTime_;
 *  std::time_t                     stopTime_;
 * */
std::tuple<std::string, JobStatus::Status,
           folly::Optional<std::time_t>,
           folly::Optional<std::time_t>>
TaskDescription::parseVal(const folly::StringPiece& rawVal) {
    int offset = 0;
    size_t len = *reinterpret_cast<const size_t*>(rawVal.begin() + offset);
    offset += sizeof(size_t);
    std::string dest(rawVal.begin() + offset, len);
    offset += len;
    JobStatus::Status status = *reinterpret_cast<const JobStatus::Status*>(rawVal.begin() + offset);
    offset += sizeof(JobStatus::Status);
    auto tStart = *reinterpret_cast<const folly::Optional<std::time_t>*>(rawVal.begin() + offset);
    offset += sizeof(folly::Optional<std::time_t>);
    auto tStop = *reinterpret_cast<const folly::Optional<std::time_t>*>(rawVal.begin() + offset);
    return std::make_tuple(dest, status, tStart, tStop);
}

/*
 * =====================================================================================
 * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time         |
 * =====================================================================================
 * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19 11:09:40 |
 * -------------------------------------------------------------------------------------
 * */
std::vector<std::string> TaskDescription::dump() {
    std::vector<std::string> ret;
    ret.emplace_back(folly::stringPrintf("%d-%d", iJob_, iTask_));
    ret.emplace_back(dest_);
    ret.emplace_back(JobStatus::toString(status_));
    ret.emplace_back(JobUtil::strTimeT(startTime_));
    ret.emplace_back(JobUtil::strTimeT(stopTime_));
    return ret;
}

bool TaskDescription::setStatus(JobStatus::Status newStatus) {
    if (JobStatus::laterThan(status_, newStatus)) {
        return false;
    }
    status_ = newStatus;
    if (newStatus == JobStatus::Status::RUNNING) {
        startTime_ = std::time(nullptr);
    }

    if (JobStatus::laterThan(newStatus, JobStatus::Status::RUNNING)) {
        stopTime_ = std::time(nullptr);
    }
    return true;
}

}  // namespace meta
}  // namespace nebula

