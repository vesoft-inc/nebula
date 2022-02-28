/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/AddHostsIntoZoneProcessor.h"

namespace nebula {
namespace meta {

void AddHostsIntoZoneProcessor::process(const cpp2::AddHostsIntoZoneReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto hosts = req.get_hosts();

  // Confirm that there are no duplicates in the parameters.
  if (std::unique(hosts.begin(), hosts.end()) != hosts.end()) {
    LOG(INFO) << "Hosts have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  // Confirm that the parameter is not empty.
  if (hosts.empty()) {
    LOG(INFO) << "Hosts is empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  auto zoneName = req.get_zone_name();
  auto isNew = req.get_is_new();
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  std::vector<kvstore::KV> data;
  for (auto& host : hosts) {
    // Ensure that the node is not registered.
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    if (machineExist(machineKey) == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "The host " << host << " have existed!";
      code = nebula::cpp2::ErrorCode::E_EXISTED;
      break;
    }
    data.emplace_back(MetaKeyUtils::machineKey(host.host, host.port), "");
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  std::vector<HostAddr> zoneHosts;
  if (isNew) {
    // If you are creating a new zone, should make sure the zone not existed.
    if (nebula::ok(zoneValueRet)) {
      LOG(INFO) << "Zone " << zoneName << " have existed";
      handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
      onFinished();
      return;
    }
  } else {
    // If you are appending host into a zone, should make sure the zone have existed.
    if (!nebula::ok(zoneValueRet)) {
      code = nebula::error(zoneValueRet);
      if (code == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        code = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
      }
      LOG(INFO) << "Get zone " << zoneName << " failed, error "
                << apache::thrift::util::enumNameSafe(code);
      handleErrorCode(code);
      onFinished();
      return;
    }

    auto originalHosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
    zoneHosts.insert(zoneHosts.end(), originalHosts.begin(), originalHosts.end());
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Check the hosts not exist in all zones
  code = includeByZone(hosts);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  zoneHosts.insert(zoneHosts.end(), hosts.begin(), hosts.end());
  data.emplace_back(std::move(zoneKey), MetaKeyUtils::zoneVal(std::move(zoneHosts)));

  LOG(INFO) << "Add Hosts Into Zone " << zoneName;
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
