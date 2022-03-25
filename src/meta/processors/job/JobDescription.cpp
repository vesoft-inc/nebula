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

namespace nebula {
namespace meta {

using Status = cpp2::JobStatus;
using JobType = cpp2::JobType;

JobDescription::JobDescription(GraphSpaceID space,
                               JobID jobId,
                               cpp2::JobType type,
                               std::vector<std::string> paras,
                               Status status,
                               int64_t startTime,
                               int64_t stopTime,
                               nebula::cpp2::ErrorCode errCode)
    : space_(space),
      jobId_(jobId),
      type_(type),
      paras_(std::move(paras)),
      status_(status),
      startTime_(startTime),
      stopTime_(stopTime),
      errCode_(errCode) {}

ErrorOr<nebula::cpp2::ErrorCode, JobDescription> JobDescription::makeJobDescription(
    folly::StringPiece rawkey, folly::StringPiece rawval) {
  try {
    if (!MetaKeyUtils::isJobKey(rawkey)) {
      return nebula::cpp2::ErrorCode::E_INVALID_JOB;
    }
    auto spaceIdAndJob = MetaKeyUtils::parseJobKey(rawkey);
    auto tup = MetaKeyUtils::parseJobVal(rawval);

    auto type = std::get<0>(tup);
    auto paras = std::get<1>(tup);
    for (auto p : paras) {
      LOG(INFO) << "p = " << p;
    }
    auto status = std::get<2>(tup);
    auto startTime = std::get<3>(tup);
    auto stopTime = std::get<4>(tup);
    auto errCode = std::get<5>(tup);
    return JobDescription(spaceIdAndJob.first,
                          spaceIdAndJob.second,
                          type,
                          paras,
                          status,
                          startTime,
                          stopTime,
                          errCode);
  } catch (std::exception& ex) {
    LOG(INFO) << ex.what();
  }
  return nebula::cpp2::ErrorCode::E_INVALID_JOB;
}

cpp2::JobDesc JobDescription::toJobDesc() {
  cpp2::JobDesc ret;
  ret.space_id_ref() = space_;
  ret.job_id_ref() = jobId_;
  ret.type_ref() = type_;
  ret.paras_ref() = paras_;
  ret.status_ref() = status_;
  ret.start_time_ref() = startTime_;
  ret.stop_time_ref() = stopTime_;
  ret.code_ref() = errCode_;
  return ret;
}

bool JobDescription::setStatus(Status newStatus, bool force) {
  if (JobStatus::laterThan(status_, newStatus) && !force) {
    return false;
  }
  status_ = newStatus;
  if (newStatus == Status::RUNNING || (newStatus == Status::STOPPED && startTime_ == 0)) {
    startTime_ = std::time(nullptr);
  }
  if (JobStatus::laterThan(newStatus, Status::RUNNING)) {
    stopTime_ = std::time(nullptr);
  }
  return true;
}

ErrorOr<nebula::cpp2::ErrorCode, JobDescription> JobDescription::loadJobDescription(
    GraphSpaceID space, JobID iJob, nebula::kvstore::KVStore* kv) {
  auto key = MetaKeyUtils::jobKey(space, iJob);
  std::string val;
  auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND)
      retCode = nebula::cpp2::ErrorCode::E_JOB_NOT_IN_SPACE;
    LOG(INFO) << "Loading job description failed, error: "
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  return makeJobDescription(key, val);
}

}  // namespace meta
}  // namespace nebula
