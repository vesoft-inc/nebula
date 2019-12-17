/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "folly/String.h"
#include "meta/processors/jobMan/TaskDescription.h"

namespace nebula {
namespace meta {

const char* TaskDescription::delim = ",";
// task will record like
// 1. status,host,Start Time,Stop Time
TaskDescription::TaskDescription(const std::string& sTask) {
    std::vector<std::string> paras;
    folly::split(delim, sTask, paras, true);

    size_t n = 0;
    if (n < paras.size()) { status = paras[n++]; }
    if (n < paras.size()) { dest = paras[n++]; }
    if (n < paras.size()) { startTime = paras[n++]; }
    if (n < paras.size()) { stopTime = paras[n++]; }
}

std::string TaskDescription::toString() {
    std::stringstream oss;
    oss << status << delim
        << dest << delim
        << startTime << delim
        << stopTime;
    return oss.str();
}

}  // namespace meta
}  // namespace nebula

