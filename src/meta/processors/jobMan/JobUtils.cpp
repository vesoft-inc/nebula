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
 * It it the key to describe the current job id.
 * each time a job added, this value will +1
 * *important* *important* *important*
 * this key is carefully designed to be the length of jobKey + size(int32_t) + 1
 * */
const std::string kCurrJobkey      = "__job_mgr____id"; // NOLINT

/*
 * this is the prefix for a regular job
 * there will be one job, and a bunch of tasks use this prefix
 * if there are 1 job(let say 65536) which has 4 sub tasks, there will 5 records in kvstore
 * which is
 * __job_mgr_<65536>
 * __job_mgr_<65536><0>
 * __job_mgr_<65536><1>
 * __job_mgr_<65536><2>
 * __job_mgr_<65536><3>
 * */
const std::string kJobKey          = "__job_mgr_"; // NOLINT

/*
 * DBA may call "backup jobs <from> <to>"
 * then all the jobs(and sub tasks) in the range will be moved from kJobKey to this
 * */
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

std::string JobUtil::strTimeT(const folly::Optional<std::time_t>& t) {
    return t ? strTimeT(*t) : "";
}

}  // namespace meta
}  // namespace nebula

#endif
