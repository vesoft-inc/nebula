/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "folly/String.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/JobDescription.h"

namespace nebula {
namespace meta {

JobDescription::JobDescription(int32_t id,
                               const std::string& cmd,
                               std::vector<std::string>& paras)
                               : id_(id),
                                 cmd_(cmd),
                                 paras_(paras),
                                 status_(JobStatus::Status::QUEUE),
                                 startTime_(folly::none),
                                 stopTime_(folly::none) {}

JobDescription::JobDescription(const folly::StringPiece& key,
                               const folly::StringPiece& val) {
    id_ = parseKey(key);
    auto tup = parseVal(val);
    cmd_ = std::get<0>(tup);
    paras_ = std::get<1>(tup);
    status_ = std::get<2>(tup);
    startTime_ = std::get<3>(tup);
    stopTime_ = std::get<4>(tup);
}

std::string JobDescription::jobKey() const {
    return makeJobKey(id_);
}

std::string JobDescription::makeJobKey(int32_t iJob) {
    std::string str;
    str.reserve(32);
    str.append(reinterpret_cast<const char*>(JobUtil::jobPrefix().data()),
                                             JobUtil::jobPrefix().size());
    str.append(reinterpret_cast<const char*>(&iJob), sizeof(int32_t));
    return str;
}

int32_t JobDescription::parseKey(const folly::StringPiece& rawKey) {
    auto offset = JobUtil::jobPrefix().size();
    if (offset >= rawKey.size()) { return 0; }
    return *reinterpret_cast<const int32_t*>(rawKey.begin() + offset);
}

std::string JobDescription::jobVal() const {
    using typeT = folly::Optional<std::time_t>;
    std::string str;
    auto cmdLen = cmd_.length();
    auto paraSize = paras_.size();
    str.reserve(256);
    str.append(reinterpret_cast<const char*>(&cmdLen), sizeof(cmdLen));
    str.append(reinterpret_cast<const char*>(&cmd_[0]), cmd_.length());
    str.append(reinterpret_cast<const char*>(&paraSize), sizeof(paraSize));
    for (auto& para : paras_) {
        auto len = para.length();
        str.append(reinterpret_cast<const char*>(&len), sizeof(len));
        str.append(reinterpret_cast<const char*>(&para[0]), len);
    }
    str.append(reinterpret_cast<const char*>(&status_), sizeof(JobStatus::Status));
    str.append(reinterpret_cast<const char*>(&startTime_), sizeof(typeT));
    str.append(reinterpret_cast<const char*>(&stopTime_), sizeof(typeT));
    return str;
}

std::tuple<std::string,
           std::vector<std::string>,
           JobStatus::Status,
           folly::Optional<std::time_t>,
           folly::Optional<std::time_t>>
JobDescription::parseVal(const folly::StringPiece& rawVal) {
    using typeT = folly::Optional<std::time_t>;
    int32_t offset = 0;
    size_t cmdLen = *reinterpret_cast<const size_t*>(rawVal.begin() + offset);
    offset += sizeof(size_t);
    std::string cmd(rawVal.begin() + offset, cmdLen);
    offset += cmdLen;
    size_t sizeOfParas = *reinterpret_cast<const size_t*>(rawVal.begin() + offset);
    offset += sizeof(size_t);
    std::vector<std::string> paras(sizeOfParas);
    for (size_t i = 0; i != sizeOfParas; ++i) {
        size_t len = *reinterpret_cast<const size_t*>(rawVal.begin() + offset);
        offset += sizeof(size_t);
        paras[i] = std::string(rawVal.begin() + offset, len);
        offset += len;
    }
    JobStatus::Status status = *reinterpret_cast<const JobStatus::Status*>(rawVal.begin() + offset);
    offset += sizeof(JobStatus::Status);
    typeT tStart = *reinterpret_cast<const typeT*>(rawVal.begin() + offset);
    offset += sizeof(typeT);
    typeT tStop = *reinterpret_cast<const typeT*>(rawVal.begin() + offset);
    return std::make_tuple(cmd, paras, status, tStart, tStop);
}

std::vector<std::string> JobDescription::dump() {
    std::vector<std::string> ret;
    ret.emplace_back(std::to_string(id_));
    std::stringstream oss;
    oss << cmd_ << " ";
    for (auto& p : paras_) {
        oss << p << " ";
    }
    ret.emplace_back(oss.str());
    ret.emplace_back(JobStatus::toString(status_));
    ret.emplace_back(JobUtil::strTimeT(startTime_));
    ret.emplace_back(JobUtil::strTimeT(stopTime_));
    return ret;
}

const std::string& JobDescription::jobPrefix() {
    return JobUtil::jobPrefix();
}

const std::string& JobDescription::currJobKey() {
    return JobUtil::currJobKey();
}

std::string JobDescription::archiveKey() {
    std::string str;
    str.reserve(32);
    str.append(reinterpret_cast<const char*>(JobUtil::archivePrefix().data()),
                                             JobUtil::archivePrefix().size());
    str.append(reinterpret_cast<const char*>(&id_), sizeof(id_));
    return str;
}

bool JobDescription::setStatus(JobStatus::Status newStatus) {
    if (JobStatus::laterThan(status_, newStatus)) {
        return false;
    }
    status_ = newStatus;
    if (newStatus == JobStatus::Status::RUNNING) {
        startTime_ = std::time(nullptr);
    }
    if (JobStatus::laterThan(newStatus, JobStatus::Status::RUNNING)) {
        stopTime_ = std::time(nullptr);
    }
    return true;
}

bool JobDescription::isJobKey(const folly::StringPiece& rawKey) {
    return rawKey.size() == JobUtil::jobPrefix().length() + sizeof(int32_t);
}

}  // namespace meta
}  // namespace nebula

