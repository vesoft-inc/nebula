/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_JOBUTILS_H_
#define META_JOBUTILS_H_

#include "meta/processors/jobMan/JobUtils.h"

namespace nebula {
namespace meta {

/*
 * kCurrIdkey before kJobKey
 * kJobKey before kJobArchiveKey
 * 
 * */
const std::string kCurrJobkey      = "__job_mgr____id"; // NOLINT
const std::string kJobKey          = "__job_mgr_"; // NOLINT
const std::string kJobArchiveKey   = "__job_mgr_archive_"; // NOLINT

const std::string& JobUtil::jobPrefix() {
    return kJobKey;
}

const std::string& JobUtil::currJobKey() {
    return kCurrJobkey;
}

const std::string& JobUtil::archivePrefix() {
    return kJobArchiveKey;
}

std::string JobUtil::strTimeT(std::time_t t) {
    std::time_t tm = t;
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

#endif
