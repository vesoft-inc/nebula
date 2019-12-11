/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "folly/String.h"
#include "meta/JobDescription.h"

namespace nebula {
namespace meta {

const char* JobDescription::delim = ",";
// job will record like
// 1. compact my_space2
// 2. flush my_space3
JobDescription::JobDescription(const std::string& sJobDesc) {
    std::vector<std::string> paras;
    folly::split(delim, sJobDesc, paras, true);

    size_t n = 0;
    if (n < paras.size()) { type = paras[n++]; }
    if (n < paras.size()) { para = paras[n++]; }
    if (n < paras.size()) { status = paras[n++]; }
    if (n < paras.size()) { startTime = paras[n++]; }
    if (n < paras.size()) { stopTime = paras[n++]; }
}

std::string JobDescription::toString() {
    std::stringstream oss;
    oss << type << delim
        << para << delim
        << status << delim
        << startTime << delim
        << stopTime;
    return oss.str();
}

}  // namespace meta
}  // namespace nebula

