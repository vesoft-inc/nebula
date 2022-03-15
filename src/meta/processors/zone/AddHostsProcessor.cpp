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
  for (auto& host : hosts) {
    // Ensure that the node is not registered.
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    if (machineExist(machineKey) == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "The host " << host << " have existed!";
      code = nebula::cpp2::ErrorCode::E_EXISTED;
      break;
    }

    // Automatic generation zone
    auto zoneName = folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
    auto zoneIdRet = getZoneId(zoneName);
    if (nebula::ok(zoneIdRet)) {
      LOG(ERROR) << "Zone " << zoneName << " have existed";
      code = nebula::error(zoneIdRet);
      break;
    }

    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    auto zoneVal = MetaKeyUtils::zoneVal({host});
    data.emplace_back(std::move(machineKey), "");
    data.emplace_back(std::move(zoneKey), std::move(zoneVal));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  auto ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
