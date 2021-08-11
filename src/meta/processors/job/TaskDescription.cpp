/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/TaskDescription.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "common/time/WallClock.h"
#include "meta/MetaServiceUtils.h"
#include <folly/String.h>

namespace nebula {
namespace meta {

using Status = cpp2::JobStatus;
using Host = std::pair<int, int>;

TaskDescription::TaskDescription(JobID iJob, TaskID iTask, const HostAddr& dst)
                                : TaskDescription(iJob, iTask, dst.host, dst.port) {}

TaskDescription::TaskDescription(JobID iJob, TaskID iTask, std::string addr, int32_t port)
                : iJob_(iJob)
                , iTask_(iTask)
                , dest_(addr, port)
                , status_(cpp2::JobStatus::RUNNING)
                , startTime_(std::time(nullptr))
                , stopTime_(0) {}


/*
 * JobID                           iJob_;
 * TaskID                          iTask_;
 * HostAddr                        dest_;
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
                                             JobUtil::jobPrefix().size())
       .append(reinterpret_cast<const char*>(&iJob_), sizeof(JobID))
       .append(reinterpret_cast<const char*>(&iTask_), sizeof(TaskID));
    return str;
}

std::pair<JobID, TaskID>
TaskDescription::parseKey(const folly::StringPiece& rawKey) {
    auto offset = JobUtil::jobPrefix().size();
    JobID iJob =  *reinterpret_cast<const JobID*>(rawKey.begin() + offset);
    offset += sizeof(JobID);
    TaskID iTask = *reinterpret_cast<const int32_t*>(rawKey.begin() + offset);
    return std::make_pair(iJob, iTask);
}

std::string TaskDescription::archiveKey() {
    std::string str;
    str.reserve(32);
    str.append(reinterpret_cast<const char*>(JobUtil::archivePrefix().data()),
                                             JobUtil::archivePrefix().size())
       .append(reinterpret_cast<const char*>(&iJob_), sizeof(JobID))
       .append(reinterpret_cast<const char*>(&iTask_), sizeof(TaskID));
    return str;
}

std::string TaskDescription::taskVal() {
    std::string str;
    str.reserve(128);
    str.append(MetaServiceUtils::serializeHostAddr(dest_))
       .append(reinterpret_cast<const char*>(&status_), sizeof(Status))
       .append(reinterpret_cast<const char*>(&startTime_), sizeof(startTime_))
       .append(reinterpret_cast<const char*>(&stopTime_), sizeof(stopTime_));
    return str;
}

/*
 * HostAddr                        dest_;
 * cpp2::JobStatus                 status_;
 * int64_t                         startTime_;
 * int64_t                         stopTime_;
 * */
// std::tuple<Host, Status, int64_t, int64_t>
std::tuple<HostAddr, Status, int64_t, int64_t>
TaskDescription::parseVal(const folly::StringPiece& rawVal) {
    size_t offset = 0;

    folly::StringPiece raw = rawVal;
    HostAddr host = MetaServiceUtils::deserializeHostAddr(raw);
    offset += sizeof(size_t);
    offset += host.host.size();
    offset += sizeof(uint32_t);

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

