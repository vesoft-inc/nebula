/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/UpdateZoneProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void AddHostIntoZoneProcessor::process(const cpp2::AddHostIntoZoneReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
  auto zoneName = req.get_zone_name();
  auto zoneIdRet = getZoneId(zoneName);
  if (!nebula::ok(zoneIdRet)) {
    auto retCode = nebula::error(zoneIdRet);
    LOG(ERROR) << "Get Zone failed, zone " << zoneName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (!nebula::ok(zoneValueRet)) {
    auto retCode = nebula::error(zoneValueRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
    }
    LOG(ERROR) << "Get zone " << zoneName << " failed, error "
               << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
  auto host = req.get_node();
  // check this host not exist in all zones
  const auto& prefix = MetaKeyUtils::zonePrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "Get zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto name = MetaKeyUtils::parseZoneName(iter->key());
    auto zoneHosts = MetaKeyUtils::parseZoneHosts(iter->val());
    auto hostIter = std::find(zoneHosts.begin(), zoneHosts.end(), host);
    if (hostIter != zoneHosts.end()) {
      LOG(ERROR) << "Host overlap found in zone " << name;
      handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
      onFinished();
      return;
    }
    iter->next();
  }

  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  if (!nebula::ok(activeHostsRet)) {
    auto retCode = nebula::error(activeHostsRet);
    LOG(ERROR) << "Get hosts failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto activeHosts = nebula::value(activeHostsRet);

  auto found = std::find(activeHosts.begin(), activeHosts.end(), host);
  if (found == activeHosts.end()) {
    LOG(ERROR) << "Host " << host << " not exist";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  hosts.emplace_back(host);
  std::vector<kvstore::KV> data;
  data.emplace_back(std::move(zoneKey), MetaKeyUtils::zoneVal(std::move(hosts)));
  LOG(INFO) << "Add Host " << host << " Into Zone " << zoneName;
  doSyncPutAndUpdate(std::move(data));
}

void DropHostFromZoneProcessor::process(const cpp2::DropHostFromZoneReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
  auto zoneName = req.get_zone_name();
  auto zoneIdRet = getZoneId(zoneName);
  if (!nebula::ok(zoneIdRet)) {
    auto retCode = nebula::error(zoneIdRet);
    LOG(ERROR) << "Get Zone failed, group " << zoneName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (!nebula::ok(zoneValueRet)) {
    auto retCode = nebula::error(zoneValueRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
    }
    LOG(ERROR) << "Get zone " << zoneName
               << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto spaceIterRet = doPrefix(spacePrefix);
  auto spaceIter = nebula::value(spaceIterRet).get();
  auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
  while (spaceIter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(spaceIter->key());
    auto spaceKey = MetaKeyUtils::spaceKey(spaceId);
    auto ret = doGet(spaceKey);
    if (!nebula::ok(ret)) {
      code = nebula::error(ret);
      LOG(ERROR) << "Get Space " << spaceId
                 << " error: " << apache::thrift::util::enumNameSafe(code);
      break;
    }

    auto properties = MetaKeyUtils::parseSpace(nebula::value(ret));

    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    auto partIterRet = doPrefix(partPrefix);
    auto partIter = nebula::value(partIterRet).get();

    while (partIter->valid()) {
      auto partHosts = MetaKeyUtils::parsePartVal(partIter->val());
      for (auto& h : partHosts) {
        if (h == req.get_node()) {
          LOG(ERROR) << h << " is related with partition";
          code = nebula::cpp2::ErrorCode::E_CONFLICT;
          break;
        }
      }

      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        break;
      } else {
        partIter->next();
      }
    }

    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      break;
    } else {
      spaceIter->next();
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
    onFinished();
    return;
  }

  auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
  auto host = req.get_node();
  auto iter = std::find(hosts.begin(), hosts.end(), host);
  if (iter == hosts.end()) {
    LOG(ERROR) << "Host " << host << " not exist in the zone " << zoneName;
    handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
    onFinished();
    return;
  }

  hosts.erase(iter);
  std::vector<kvstore::KV> data;
  data.emplace_back(std::move(zoneKey), MetaKeyUtils::zoneVal(std::move(hosts)));
  LOG(INFO) << "Drop Host " << host << " From Zone " << zoneName;
  doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
