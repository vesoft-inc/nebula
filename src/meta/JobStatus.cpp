/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "folly/String.h"
#include "meta/JobStatus.h"

namespace nebula {
namespace meta {

int JobStatus::phaseNumber(const std::string& status) {
    if (status == JobStatus::queue) return 0;
    if (status == JobStatus::running) return 1;
    if (status == JobStatus::finished) return 2;
    if (status == JobStatus::failed) return 2;
    if (status == JobStatus::stopped) return INT_MAX;
    return INT_MIN;
}

bool JobStatus::laterThan(const std::string& lhs, const std::string& rhs) {
    return phaseNumber(lhs) > phaseNumber(rhs);
}

const char* JobStatus::queue = "queue";
const char* JobStatus::running = "running";
const char* JobStatus::finished = "finished";
const char* JobStatus::failed = "failed";
const char* JobStatus::stopped = "stopped";

}  // namespace meta
}  // namespace nebula

