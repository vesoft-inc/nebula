/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/AddHostsProcessor.h"

#include "version/Version.h"

DECLARE_uint32(expired_time_factor);
DECLARE_int32(removed_threshold_sec);

namespace nebula {
namespace meta {

void AddHostsProcessor::process(const cpp2::AddHostsReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto hosts = req.get_hosts();
  // Confirm that there are no duplicates in the parameters.
  if (std::unique(hosts.begin(), hosts.end()) != hosts.end()) {
    LOG(ERROR) << "Hosts have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  // Confirm that the parameter is not empty.
  if (hosts.empty()) {
    LOG(ERROR) << "Hosts is empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  std::map<GraphSpaceID, meta::cpp2::SpaceDesc> spaceMap;
  std::string spacePrefix = MetaKeyUtils::spacePrefix();
  std::unique_ptr<kvstore::KVIterator> spaceIter;
  auto spaceRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &spaceIter);
  if (spaceRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(spaceRet);
    onFinished();
    return;
  }

  while (spaceIter->valid()) {
    spaceMap.emplace(MetaKeyUtils::spaceId(spaceIter->key()),
                     MetaKeyUtils::parseSpace(spaceIter->val()));
    spaceIter->next();
  }
  for (auto& host : hosts) {
    auto hostKey = MetaKeyUtils::hostKey(host.host, host.port);
    auto hostRes = doGet(hostKey);
    // check if the host is storage, if it is not storage, it should be found in host table not
    // expired, so it can't be added
    if (nebula::ok(hostRes)) {
      auto now = time::WallClock::fastNowInMilliSec();
      auto hostInfo = HostInfo::decode(nebula::value(hostRes));
      if (hostInfo.role_ != cpp2::HostRole::STORAGE &&
          now - hostInfo.lastHBTimeInMilliSec_ <
              FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor * 1000) {
        LOG(ERROR) << "The host " << host << " is not storage";
        code = nebula::cpp2::ErrorCode::E_HOST_CAN_NOT_BE_ADDED;
        break;
      }
    }

    // Ensure that the node is not registered.
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    if (machineExist(machineKey) == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "The host " << host << " have existed!";
      code = nebula::cpp2::ErrorCode::E_EXISTED;
      break;
    }

    // Automatic generation zone
    auto zoneName = folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
    auto zoneRet = zoneExist(zoneName);
    if (zoneRet != nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND) {
      if (zoneRet == nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Zone " << zoneName << " have existed";
        zoneRet = nebula::cpp2::ErrorCode::E_KEY_HAS_EXISTS;
      }
      code = zoneRet;
      break;
    }

    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    auto zoneVal = MetaKeyUtils::zoneVal({host});
    data.emplace_back(std::move(machineKey), "");
    data.emplace_back(std::move(zoneKey), std::move(zoneVal));
    for (auto& [spaceId, properties] : spaceMap) {
      const std::vector<std::string>& curZones = properties.get_zone_names();
      std::set<std::string> zm(curZones.begin(), curZones.end());
      zm.emplace(zoneName);
      std::vector<std::string> newZones(zm.begin(), zm.end());
      properties.zone_names_ref() = std::move(newZones);
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  for (auto& [spaceId, properties] : spaceMap) {
    data.emplace_back(MetaKeyUtils::spaceKey(spaceId), MetaKeyUtils::spaceVal(properties));
  }
  auto ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
