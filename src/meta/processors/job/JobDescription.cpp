/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/JobDescription.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include <boost/stacktrace.hpp>
#include <stdexcept>

#include "common/utils/MetaKeyUtils.h"
#include "kvstore/KVIterator.h"
#include "meta/processors/Common.h"
#include "meta/processors/job/JobUtils.h"

namespace nebula {
namespace meta {

using Status = cpp2::JobStatus;
using JobType = cpp2::JobType;

int32_t JobDescription::minDataVer_ = 1;
int32_t JobDescription::currDataVer_ = 1;

JobDescription::JobDescription(JobID id,
                               cpp2::JobType type,
                               std::vector<std::string> paras,
                               Status status,
                               int64_t startTime,
                               int64_t stopTime)
    : id_(id),
      type_(type),
      paras_(std::move(paras)),
      status_(status),
      startTime_(startTime),
      stopTime_(stopTime) {}

ErrorOr<nebula::cpp2::ErrorCode, JobDescription> JobDescription::makeJobDescription(
    folly::StringPiece rawkey, folly::StringPiece rawval) {
  try {
    if (!isJobKey(rawkey)) {
      return nebula::cpp2::ErrorCode::E_INVALID_JOB;
    }
    auto key = parseKey(rawkey);

    if (!isSupportedValue(rawval)) {
      LOG(INFO) << "not supported data ver of job " << key;
      return nebula::cpp2::ErrorCode::E_INVALID_JOB;
    }
    auto tup = parseVal(rawval);

    auto type = std::get<0>(tup);
    auto paras = std::get<1>(tup);
    for (auto p : paras) {
      LOG(INFO) << "p = " << p;
    }
    auto status = std::get<2>(tup);
    auto startTime = std::get<3>(tup);
    auto stopTime = std::get<4>(tup);
    return JobDescription(key, type, paras, status, startTime, stopTime);
  } catch (std::exception& ex) {
    LOG(INFO) << ex.what();
  }
  return nebula::cpp2::ErrorCode::E_INVALID_JOB;
}

std::string JobDescription::jobKey() const {
  return makeJobKey(id_);
}

std::string JobDescription::makeJobKey(JobID iJob) {
  std::string str;
  str.reserve(32);
  auto& jPrefix = JobUtil::jobPrefix();
  str.append(jPrefix.data(), jPrefix.size())
      .append(reinterpret_cast<const char*>(&iJob), sizeof(JobID));
  return str;
}

int32_t JobDescription::parseKey(const folly::StringPiece& rawKey) {
  auto offset = JobUtil::jobPrefix().size();
  return *reinterpret_cast<const int32_t*>(rawKey.begin() + offset);
}

std::string JobDescription::jobVal() const {
  std::string str;
  str.reserve(256);
  // use a big num to avoid possible conflict
  int32_t dataVersion = INT_MAX - currDataVer_;
  str.append(reinterpret_cast<const char*>(&dataVersion), sizeof(dataVersion))
      .append(reinterpret_cast<const char*>(&type_), sizeof(type_));
  auto paraSize = paras_.size();
  str.append(reinterpret_cast<const char*>(&paraSize), sizeof(size_t));
  for (auto& para : paras_) {
    auto len = para.length();
    str.append(reinterpret_cast<const char*>(&len), sizeof(len))
        .append(reinterpret_cast<const char*>(&para[0]), len);
  }
  str.append(reinterpret_cast<const char*>(&status_), sizeof(Status))
      .append(reinterpret_cast<const char*>(&startTime_), sizeof(int64_t))
      .append(reinterpret_cast<const char*>(&stopTime_), sizeof(int64_t));
  return str;
}

std::tuple<JobType, std::vector<std::string>, Status, int64_t, int64_t> JobDescription::parseVal(
    const folly::StringPiece& rawVal) {
  return decodeValV1(rawVal);
}

// old saved data may have different format
// which means we have different decoder for each version
std::tuple<JobType, std::vector<std::string>, Status, int64_t, int64_t> JobDescription::decodeValV1(
    const folly::StringPiece& rawVal) {
  size_t offset = sizeof(int32_t);

  auto type = JobUtil::parseFixedVal<JobType>(rawVal, offset);
  offset += sizeof(type);

  std::vector<std::string> paras = JobUtil::parseStrVector(rawVal, &offset);

  auto status = JobUtil::parseFixedVal<Status>(rawVal, offset);
  offset += sizeof(Status);

  auto tStart = JobUtil::parseFixedVal<int64_t>(rawVal, offset);
  offset += sizeof(int64_t);

  auto tStop = JobUtil::parseFixedVal<int64_t>(rawVal, offset);

  return std::make_tuple(type, paras, status, tStart, tStop);
}

cpp2::JobDesc JobDescription::toJobDesc() {
  cpp2::JobDesc ret;
  ret.id_ref() = id_;
  ret.type_ref() = type_;
  ret.paras_ref() = paras_;
  ret.status_ref() = status_;
  ret.start_time_ref() = startTime_;
  ret.stop_time_ref() = stopTime_;
  return ret;
}

std::string JobDescription::archiveKey() {
  std::string str;
  str.reserve(32);
  auto& aPrefix = JobUtil::archivePrefix();
  str.append(aPrefix.data(), aPrefix.size())
      .append(reinterpret_cast<const char*>(&id_), sizeof(id_));
  return str;
}

bool JobDescription::setStatus(Status newStatus, bool force) {
  if (JobStatus::laterThan(status_, newStatus) && !force) {
    return false;
  }
  status_ = newStatus;
  if (newStatus == Status::RUNNING) {
    startTime_ = std::time(nullptr);
  }
  if (JobStatus::laterThan(newStatus, Status::RUNNING)) {
    stopTime_ = std::time(nullptr);
  }
  return true;
}

bool JobDescription::isJobKey(const folly::StringPiece& rawKey) {
  if (!rawKey.startsWith(JobUtil::jobPrefix())) {
    return false;
  }
  return rawKey.size() == JobUtil::jobPrefix().length() + sizeof(int32_t);
}

ErrorOr<nebula::cpp2::ErrorCode, JobDescription> JobDescription::loadJobDescription(
    JobID iJob, nebula::kvstore::KVStore* kv) {
  auto key = makeJobKey(iJob);
  std::string val;
  auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Loading Job Description Failed" << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  return makeJobDescription(key, val);
}

bool JobDescription::isSupportedValue(const folly::StringPiece& val) {
  if (val.size() < sizeof(int32_t)) {
    return false;
  }

  int32_t ver = INT_MAX - JobUtil::parseFixedVal<int32_t>(val, 0);
  return ver >= minDataVer_ && ver <= currDataVer_;
}

}  // namespace meta
}  // namespace nebula
