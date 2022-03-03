/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/ClearSpaceProcessor.h"

namespace nebula {
namespace meta {

void ClearSpaceProcessor::process(const cpp2::ClearSpaceReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
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

  auto spaceId = nebula::value(spaceRet);
  auto ret = getAllParts(spaceId);
  if (!nebula::ok(ret)) {
    handleErrorCode(nebula::error(ret));
    onFinished();
    return;
  }

  std::vector<HostAddr> spaceDistributedOnHosts;
  for (auto& partEntry : nebula::value(ret)) {
    for (auto& host : partEntry.second) {
      if (std::find(spaceDistributedOnHosts.begin(), spaceDistributedOnHosts.end(), host) ==
          spaceDistributedOnHosts.end()) {
        spaceDistributedOnHosts.push_back(host);
      }
    }
  }
}

ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<PartitionID, std::vector<HostAddr>>>
ClearSpaceProcessor::getAllParts(GraphSpaceID spaceId) {
  std::unordered_map<PartitionID, std::vector<HostAddr>> partHostsMap;

  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List Parts Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    std::vector<HostAddr> partHosts = MetaKeyUtils::parsePartVal(iter->val());
    partHostsMap.emplace(partId, std::move(partHosts));
    iter->next();
  }

  return partHostsMap;
}

}  // namespace meta
}  // namespace nebula
