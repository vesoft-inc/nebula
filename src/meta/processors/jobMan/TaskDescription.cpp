/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "folly/String.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace meta {

using Status = cpp2::JobStatus;

TaskDescription::TaskDescription(int32_t iJob,
                                 int32_t iTask,
                                 const nebula::cpp2::HostAddr& dest)
                                 : iJob_(iJob),
                                   iTask_(iTask),
                                   dest_(dest),
                                   status_(cpp2::JobStatus::RUNNING),
                                   startTime_(std::time(nullptr)),
                                   stopTime_(0) {}


/*
 * int32_t                         iJob_;
 * int32_t                         iTask_;
 * nebula::cpp2::HostAddr          dest_;
 * cpp2::JobStatus                 status_;
 * int64_t                         startTime_;
 * int64_t                         stopTime_;
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
    str.append(reinterpret_cast<const char*>(&dest_), sizeof(dest_));
    str.append(reinterpret_cast<const char*>(&status_), sizeof(Status));
    str.append(reinterpret_cast<const char*>(&startTime_), sizeof(startTime_));
    str.append(reinterpret_cast<const char*>(&stopTime_), sizeof(stopTime_));
    return str;
}

/*
 * nebula::cpp2::HostAddr          dest_;
 * cpp2::JobStatus                 status_;
 * int64_t                         startTime_;
 * int64_t                         stopTime_;
 * */
std::tuple<nebula::cpp2::HostAddr, Status, int64_t, int64_t>
TaskDescription::parseVal(const folly::StringPiece& rawVal) {
    size_t offset = 0;

    auto host = JobUtil::parseFixedVal<nebula::cpp2::HostAddr>(rawVal, offset);
    offset += sizeof(host);

    auto status = JobUtil::parseFixedVal<Status>(rawVal, offset);
    offset += sizeof(Status);

    auto tStart = JobUtil::parseFixedVal<int64_t>(rawVal, offset);
    offset += sizeof(int64_t);

    auto tStop = JobUtil::parseFixedVal<int64_t>(rawVal, offset);

    return std::make_tuple(host, status, tStart, tStop);
}

/*
 * =====================================================================================
 * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time         |
 * =====================================================================================
 * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19 11:09:40 |
 * -------------------------------------------------------------------------------------
 * */
cpp2::TaskDesc TaskDescription::toTaskDesc() {
    cpp2::TaskDesc ret;
    ret.set_job_id(iJob_);
    ret.set_task_id(iTask_);
    ret.set_host(dest_);
    ret.set_status(status_);
    ret.set_start_time(startTime_);
    ret.set_stop_time(stopTime_);
    return ret;
}

bool TaskDescription::setStatus(Status newStatus) {
    if (JobStatus::laterThan(status_, newStatus)) {
        return false;
    }
    status_ = newStatus;
    if (newStatus == Status::RUNNING) {
        startTime_ = nebula::time::WallClock::fastNowInSec();
    }

    if (JobStatus::laterThan(newStatus, Status::RUNNING)) {
        stopTime_ = nebula::time::WallClock::fastNowInSec();
    }
    return true;
}

}  // namespace meta
}  // namespace nebula

