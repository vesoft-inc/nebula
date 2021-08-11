/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "meta/processors/jobMan/JobUtils.h"
#include <stdexcept>
#include <vector>
#include <boost/stacktrace.hpp>

namespace nebula {
namespace meta {

/*
 * It it the key to describe the current job id.
 * each time a job added, this value will +1
 * *important* *important* *important*
 * this key is carefully designed to be the length of jobKey + size(int32_t) + 1
 * */
const std::string kCurrJob      = "__job_mgr____id"; // NOLINT

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
const std::string kJob          = "__job_mgr_"; // NOLINT

/*
 * DBA may call "backup jobs <from> <to>"
 * then all the jobs(and sub tasks) in the range will be moved from kJob to this
 * */
const std::string kJobArchive   = "__job_mgr_archive_"; // NOLINT

const std::string& JobUtil::jobPrefix() {
    return kJob;
}

const std::string& JobUtil::currJobKey() {
    return kCurrJob;
}

const std::string& JobUtil::archivePrefix() {
    return kJobArchive;
}

std::string JobUtil::parseString(folly::StringPiece rawVal, size_t offset) {
    if (rawVal.size() < offset + sizeof(size_t)) {
        LOG(ERROR) << "Error: rawVal: " << toHexStr(rawVal)
                   << ", offset: " << offset;
        throw std::runtime_error(folly::stringPrintf("%s: offset=%zu, rawVal.size()=%zu",
                                                     __func__, offset, rawVal.size()));
    }
    auto len = *reinterpret_cast<const size_t*>(rawVal.data() + offset);
    offset += sizeof(size_t);
    if (rawVal.size() < offset + len) {
        LOG(ERROR) << "Error: rawVal: " << toHexStr(rawVal)
                   << ", len: " << len
                   << ", offset: " << offset;
        throw std::runtime_error(folly::stringPrintf("%s: offset=%zu, rawVal.size()=%zu",
                                                     __func__, offset, rawVal.size()));
    }
    return std::string(rawVal.data() + offset, len);
}

std::vector<std::string> JobUtil::parseStrVector(folly::StringPiece rawVal, size_t* offset) {
    std::vector<std::string> ret;
    if (rawVal.size() < *offset + sizeof(size_t)) {
        LOG(ERROR) << "Error: rawVal: " << toHexStr(rawVal)
                   << ", offset: " << offset;
        throw std::runtime_error(folly::stringPrintf("%s: offset=%zu, rawVal.size()=%zu",
                                                     __func__, *offset, rawVal.size()));
    }
    auto vec_size = *reinterpret_cast<const size_t*>(rawVal.data() + *offset);
    *offset += sizeof(size_t);
    for (size_t i = 0; i < vec_size; ++i) {
        ret.emplace_back(parseString(rawVal, *offset));
        *offset += sizeof(size_t);
        *offset += ret.back().length();
    }
    return ret;
}

}  // namespace meta
}  // namespace nebula
