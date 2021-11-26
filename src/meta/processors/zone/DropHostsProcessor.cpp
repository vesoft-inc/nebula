/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/DropHostsProcessor.h"

namespace nebula {
namespace meta {

void DropHostsProcessor::process(const cpp2::DropHostsReq& req) {
  auto hosts = req.get_hosts();
  if (std::unique(hosts.begin(), hosts.end()) != hosts.end()) {
    LOG(ERROR) << "Hosts have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (hosts.empty()) {
    LOG(ERROR) << "Hosts is empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  std::vector<std::string> data;
  // Check that partition is not held on the host
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto spaceIterRet = doPrefix(spacePrefix);
  auto spaceIter = nebula::value(spaceIterRet).get();
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
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

    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    auto partIterRet = doPrefix(partPrefix);
    auto partIter = nebula::value(partIterRet).get();
    while (partIter->valid()) {
      auto partHosts = MetaKeyUtils::parsePartVal(partIter->val());
      for (auto& h : partHosts) {
        if (std::find(hosts.begin(), hosts.end(), h) != hosts.end()) {
          LOG(ERROR) << h << " is related with partition";
          code = nebula::cpp2::ErrorCode::E_CONFLICT;
          break;
        }
      }
      partIter->next();
    }
    spaceIter->next();
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Remove points related to the zone
  const auto& prefix = MetaKeyUtils::zonePrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "List zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto hs = MetaKeyUtils::parseZoneHosts(iter->val());
    if (std::all_of(hs.begin(), hs.end(), [&hosts](auto& h) {
          return std::find(hosts.begin(), hosts.end(), h) != hosts.end();
        })) {
      auto zoneName = MetaKeyUtils::parseZoneName(iter->key());
      LOG(INFO) << "Drop zone " << zoneName;
      code = checkRelatedSpace(zoneName);
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        break;
      }
      data.emplace_back(iter->key());
    }
    iter->next();
  }

  // Detach the machine from cluster
  for (auto& host : hosts) {
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    auto ret = machineExist(machineKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "The host " << HostAddr(host.host, host.port) << " not existed!";
      code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
      break;
    }
    data.emplace_back(std::move(machineKey));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  resp_.set_code(nebula::cpp2::ErrorCode::SUCCEEDED);
  doMultiRemove(std::move(data));
}

nebula::cpp2::ErrorCode DropHostsProcessor::checkRelatedSpace(const std::string& zoneName) {
  const auto& prefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List spaces failed, error " << apache::thrift::util::enumNameSafe(retCode);
    return nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
  }

  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    size_t replicaFactor = properties.get_replica_factor();
    auto zones = properties.get_zone_names();
    LOG(INFO) << "replica_factor " << replicaFactor << " zone size " << zones.size();
    auto it = std::find(zones.begin(), zones.end(), zoneName);
    if (it != zones.end()) {
      if (zones.size() == replicaFactor) {
        LOG(ERROR) << "Zone size is same with replica factor";
        return nebula::cpp2::ErrorCode::E_CONFLICT;
      } else {
        zones.erase(it);
        properties.set_zone_names(zones);
        rewriteSpaceProperties(iter->key().data(), properties);
      }
    }
    iter->next();
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void DropHostsProcessor::rewriteSpaceProperties(const std::string& spaceKey,
                                                const meta::cpp2::SpaceDesc& properties) {
  auto spaceVal = MetaKeyUtils::spaceVal(properties);
  std::vector<kvstore::KV> data = {{spaceKey, spaceVal}};
  doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
