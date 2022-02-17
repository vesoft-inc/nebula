/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

#include <cstdlib>  // for atoi
#include <memory>   // for allocator_traits<>::value_type

#include "common/base/ErrorOr.h"  // for error, ok, value
#include "common/base/Logging.h"  // for LOG, LogMessage, _LOG_INFO

namespace nebula {
namespace meta {
class AdminClient;
}  // namespace meta

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {
class AdminClient;

SimpleConcurrentJobExecutor::SimpleConcurrentJobExecutor(JobID jobId,
                                                         kvstore::KVStore* kvstore,
                                                         AdminClient* adminClient,
                                                         const std::vector<std::string>& paras)
    : StorageJobExecutor(jobId, kvstore, adminClient, paras) {}

bool SimpleConcurrentJobExecutor::check() {
  auto parasNum = paras_.size();
  return parasNum == 1 || parasNum == 2;
}

nebula::cpp2::ErrorCode SimpleConcurrentJobExecutor::prepare() {
  std::string spaceName = paras_.back();
  auto errOrSpaceId = getSpaceIdFromName(spaceName);
  if (!nebula::ok(errOrSpaceId)) {
    LOG(INFO) << "Can't find the space: " << spaceName;
    return nebula::error(errOrSpaceId);
  }

  space_ = nebula::value(errOrSpaceId);
  ErrOrHosts errOrHost = getTargetHost(space_);
  if (!nebula::ok(errOrHost)) {
    LOG(INFO) << "Can't get any host according to space";
    return nebula::error(errOrHost);
  }

  if (paras_.size() > 1) {
    concurrency_ = std::atoi(paras_[0].c_str());
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode SimpleConcurrentJobExecutor::stop() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
