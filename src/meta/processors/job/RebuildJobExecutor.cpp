/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/RebuildJobExecutor.h"

#include <folly/Conv.h>                     // for to
#include <folly/Try.h>                      // for Try::value
#include <folly/Try.h>                      // for Try
#include <folly/futures/Future.h>           // for Future::Future<T>
#include <folly/futures/Future.h>           // for Future, collectAll
#include <folly/futures/Promise.h>          // for PromiseException::Pro...
#include <gflags/gflags_declare.h>          // for DECLARE_int32
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <algorithm>  // for any_of
#include <utility>    // for move, pair

#include "common/base/ErrorOr.h"                // for error, ok, value
#include "common/base/Logging.h"                // for LOG, LogMessage, _LOG...
#include "common/base/StatusOr.h"               // for StatusOr
#include "common/utils/MetaKeyUtils.h"          // for MetaKeyUtils, kDefaul...
#include "common/utils/Types.h"                 // for IndexID
#include "kvstore/KVStore.h"                    // for KVStore
#include "meta/processors/admin/AdminClient.h"  // for AdminClient

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

bool RebuildJobExecutor::check() {
  return paras_.size() >= 1;
}

nebula::cpp2::ErrorCode RebuildJobExecutor::prepare() {
  // the last value of paras_ is the space name, others are index name
  auto spaceRet = getSpaceIdFromName(paras_.back());
  if (!nebula::ok(spaceRet)) {
    LOG(INFO) << "Can't find the space: " << paras_.back();
    return nebula::error(spaceRet);
  }
  space_ = nebula::value(spaceRet);

  std::string indexValue;
  IndexID indexId = -1;
  for (auto i = 0u; i < paras_.size() - 1; i++) {
    auto indexKey = MetaKeyUtils::indexIndexKey(space_, paras_[i]);
    auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &indexValue);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Get indexKey error indexName: " << paras_[i]
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    indexId = *reinterpret_cast<const IndexID*>(indexValue.c_str());
    LOG(INFO) << "Rebuild Index Space " << space_ << ", Index " << indexId;
    taskParameters_.emplace_back(folly::to<std::string>(indexId));
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode RebuildJobExecutor::stop() {
  auto errOrTargetHost = getTargetHost(space_);
  if (!nebula::ok(errOrTargetHost)) {
    LOG(INFO) << "Get target host failed";
    auto retCode = nebula::error(errOrTargetHost);
    if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
    }
    return retCode;
  }

  auto& hosts = nebula::value(errOrTargetHost);
  std::vector<folly::Future<StatusOr<bool>>> futures;
  for (auto& host : hosts) {
    // Will convert StorageAddr to AdminAddr in AdminClient
    auto future = adminClient_->stopTask(host.first, jobId_, 0);
    futures.emplace_back(std::move(future));
  }

  auto tries = folly::collectAll(std::move(futures)).get();
  if (std::any_of(tries.begin(), tries.end(), [](auto& t) { return t.hasException(); })) {
    LOG(INFO) << "RebuildJobExecutor::stop() RPC failure.";
    return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
  }
  for (const auto& t : tries) {
    if (!t.value().ok()) {
      LOG(INFO) << "Stop Build Index Failed";
      return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
