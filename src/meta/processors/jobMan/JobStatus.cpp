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
using Status = cpp2::JobStatus;

int JobStatus::phaseNumber(Status st) {
    if (st == Status::QUEUE) return kNotStarted;
    if (st == Status::RUNNING) return kInProgress;
    if (st == Status::FINISHED) return kDeadEnd;
    if (st == Status::FAILED) return kDeadEnd;
    if (st == Status::STOPPED) return kDeadEnd;
    return INT_MIN;
}

bool JobStatus::laterThan(Status lhs, Status rhs) {
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

}  // namespace meta
}  // namespace nebula

