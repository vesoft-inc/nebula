/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/DropZoneProcessor.h"

namespace nebula {
namespace meta {

void DropZoneProcessor::process(const cpp2::DropZoneReq& req) {
  folly::SharedMutex::WriteHolder wHolder(LockUtils::zoneLock());
  auto zoneName = req.get_zone_name();
  auto zoneIdRet = getZoneId(zoneName);
  if (!nebula::ok(zoneIdRet)) {
    auto code = nebula::error(zoneIdRet);
    LOG(ERROR) << "Drop Zone Failed, error: " << apache::thrift::util::enumNameSafe(code);
    handleErrorCode(code);
    onFinished();
    return;
  }

  nebula::cpp2::ErrorCode code = checkSpaceReplicaZone();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }
  std::vector<std::string> keys;
  keys.emplace_back(MetaKeyUtils::indexZoneKey(zoneName));
  keys.emplace_back(MetaKeyUtils::zoneKey(zoneName));
  LOG(INFO) << "Drop Zone " << zoneName;

  // Drop zone associated hosts
  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (!nebula::ok(zoneValueRet)) {
    LOG(ERROR) << "Get zone " << zoneName << " failed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
    onFinished();
    return;
  }

  auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
  // Drop Hosts
  for (auto& host : hosts) {
    code = checkHostHostPartition(host);
  }
  doSyncMultiRemoveAndUpdate(std::move(keys));
}

nebula::cpp2::ErrorCode DropZoneProcessor::checkSpaceReplicaZone() {
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(spacePrefix);
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (!nebula::ok(ret)) {
    code = nebula::error(ret);
    LOG(ERROR) << "List spaces failed, error " << apache::thrift::util::enumNameSafe(code);
    return code;
  }

  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto spaceKey = iter->key();
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    auto spaceId = MetaKeyUtils::spaceId(spaceKey);
    auto spaceZones = properties.get_zone_names();
    size_t replicaFactor = properties.get_replica_factor();
    if (replicaFactor == spaceZones.size()) {
      LOG(ERROR) << "Space " << spaceId << " replica factor and zone size are the same";
      code = nebula::cpp2::ErrorCode::E_CONFLICT;
      break;
    }
    iter->next();
  }
  return code;
}

nebula::cpp2::ErrorCode DropZoneProcessor::checkHostHostPartition(const HostAddr& host) {
  UNUSED(host);
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
