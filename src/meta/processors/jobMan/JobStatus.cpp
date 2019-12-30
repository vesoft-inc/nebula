/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "folly/String.h"
#include "meta/processors/jobMan/JobStatus.h"

namespace nebula {
namespace meta {

const int kNotStarted = 0;
const int kInProgress = 1;
const int kDeadEnd = 2;

int JobStatus::phaseNumber(JobStatus::Status st) {
    if (st == JobStatus::Status::QUEUE) return kNotStarted;
    if (st == JobStatus::Status::RUNNING) return kInProgress;
    if (st == JobStatus::Status::FINISHED) return kDeadEnd;
    if (st == JobStatus::Status::FAILED) return kDeadEnd;
    if (st == JobStatus::Status::STOPPED) return kDeadEnd;
    return INT_MIN;
}

bool JobStatus::laterThan(JobStatus::Status lhs, JobStatus::Status rhs) {
    return phaseNumber(lhs) > phaseNumber(rhs);
}

std::string JobStatus::toString(Status st) {
    switch (st) {
    case Status::QUEUE:
        return "queue";
    case Status::RUNNING:
        return "running";
    case Status::FINISHED:
        return "finished";
    case Status::FAILED:
        return "failed";
    case Status::STOPPED:
        return "stopped";
    case Status::INVALID:
        return "invalid";
    }
    return "invalid st";
}

JobStatus::Status JobStatus::toStatus(const std::string& strStatus) {
    if (strStatus == "queue") {
        return Status::QUEUE;
    } else if (strStatus == "running") {
        return Status::RUNNING;
    } else if (strStatus == "queue") {
        return Status::QUEUE;
    } else if (strStatus == "finished") {
        return Status::FINISHED;
    } else if (strStatus == "failed") {
        return Status::FAILED;
    } else if (strStatus == "stopped") {
        return Status::STOPPED;
    } else {
        return Status::INVALID;
    }
}

}  // namespace meta
}  // namespace nebula

