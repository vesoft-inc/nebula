/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

#include "common/utils/MetaKeyUtils.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

SimpleConcurrentJobExecutor::SimpleConcurrentJobExecutor(JobID jobId,
                                                         kvstore::KVStore* kvstore,
                                                         AdminClient* adminClient,
                                                         const std::vector<std::string>& paras)
    : StorageJobExecutor(jobId, kvstore, adminClient, paras) {}

bool SimpleConcurrentJobExecutor::check() {
  auto parasNum = paras_.size();
  return parasNum == 1;
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

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode SimpleConcurrentJobExecutor::stop() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
