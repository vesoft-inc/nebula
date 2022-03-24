/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/RebuildJobExecutor.h"

#include "common/network/NetworkUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "common/utils/Utils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode RebuildJobExecutor::prepare() {
  // The last value of paras_ are index name
  auto spaceRet = spaceExist();
  if (spaceRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't find the space, spaceId " << space_;
    return spaceRet;
  }

  std::string indexValue;
  IndexID indexId = -1;
  for (auto i = 0u; i < paras_.size(); i++) {
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
