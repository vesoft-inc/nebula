/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/ClearSpaceProcessor.h"

namespace nebula {
namespace meta {

void ClearSpaceProcessor::process(const cpp2::ClearSpaceReq& req) {
  GraphSpaceID spaceId;
  std::vector<HostAddr> hosts;
  {
    folly::SharedMutex::ReadHolder holder(LockUtils::lock());

    // 1. Fetch spaceId
    const auto& spaceName = req.get_space_name();
    auto spaceRet = getSpaceId(spaceName);
    if (!nebula::ok(spaceRet)) {
      auto retCode = nebula::error(spaceRet);
      if (retCode == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
        if (req.get_if_exists()) {
          retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
        } else {
          LOG(WARNING) << "Clear space Failed, space " << spaceName << " not existed.";
        }
      } else {
        LOG(ERROR) << "Clear space Failed, space " << spaceName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
      }
      handleErrorCode(retCode);
      onFinished();
      return;
    }
    spaceId = nebula::value(spaceRet);

    // 2. Fetch all parts info according the spaceId.
    auto ret = getAllParts(spaceId);
    if (!nebula::ok(ret)) {
      handleErrorCode(nebula::error(ret));
      onFinished();
      return;
    }

    // 3. Determine which hosts the space is distributed on.
    std::unordered_set<HostAddr> distributedOnHosts;
    for (auto& partEntry : nebula::value(ret)) {
      for (auto& host : partEntry.second) {
        distributedOnHosts.insert(host);
      }
    }

    // 4. select the active hosts.
    auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
    if (!nebula::ok(activeHostsRet)) {
      handleErrorCode(nebula::error(activeHostsRet));
      onFinished();
      return;
    }
    auto activeHosts = std::move(nebula::value(activeHostsRet));
    for (auto& host : distributedOnHosts) {
      if (std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end()) {
        hosts.push_back(host);
      }
    }
    if (hosts.size() == 0) {
      handleErrorCode(nebula::cpp2::ErrorCode::E_NO_HOSTS);
      onFinished();
      return;
    }
  }

  // 5. Delete the space data on the corresponding hosts.
  auto clearRet = adminClient_->clearSpace(spaceId, hosts).get();
  handleErrorCode(clearRet);
  onFinished();
  return;
}

}  // namespace meta
}  // namespace nebula
