/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

#include "common/utils/MetaKeyUtils.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

SimpleConcurrentJobExecutor::SimpleConcurrentJobExecutor(GraphSpaceID space,
                                                         JobID jobId,
                                                         kvstore::KVStore* kvstore,
                                                         AdminClient* adminClient,
                                                         const std::vector<std::string>& paras)
    : StorageJobExecutor(space, jobId, kvstore, adminClient, paras) {}

nebula::cpp2::ErrorCode SimpleConcurrentJobExecutor::check() {
  return paras_.empty() ? nebula::cpp2::ErrorCode::SUCCEEDED
                        : nebula::cpp2::ErrorCode::E_INVALID_JOB;
}

nebula::cpp2::ErrorCode SimpleConcurrentJobExecutor::prepare() {
  auto spaceRet = spaceExist();
  if (spaceRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't find the space, spaceId " << space_;
    return spaceRet;
  }

  ErrOrHosts errOrHost = getTargetHost(space_);
  if (!nebula::ok(errOrHost)) {
    LOG(INFO) << "Can't get any host according to space";
    return nebula::error(errOrHost);
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
