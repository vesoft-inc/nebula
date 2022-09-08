/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/DropHostsProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void DropHostsProcessor::process(const cpp2::DropHostsReq& req) {
  folly::SharedMutex::WriteHolder lockHolder(LockUtils::lock());
  auto hosts = req.get_hosts();
  if (std::unique(hosts.begin(), hosts.end()) != hosts.end()) {
    LOG(INFO) << "Hosts have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (hosts.empty()) {
    LOG(INFO) << "Hosts is empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  std::vector<std::string> data;
  auto holder = std::make_unique<kvstore::BatchHolder>();
  // Check that partition is not held on the host
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto spaceIterRet = doPrefix(spacePrefix);
  auto spaceIter = nebula::value(spaceIterRet).get();
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  std::map<GraphSpaceID, meta::cpp2::SpaceDesc> spaceMap;
  while (spaceIter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(spaceIter->key());
    auto spaceKey = MetaKeyUtils::spaceKey(spaceId);
    auto ret = doGet(spaceKey);
    if (!nebula::ok(ret)) {
      code = nebula::error(ret);
      LOG(INFO) << "Get Space " << spaceId
                << " error: " << apache::thrift::util::enumNameSafe(code);
      break;
    }
    spaceMap.emplace(spaceId, MetaKeyUtils::parseSpace(spaceIter->val()));
    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    auto partIterRet = doPrefix(partPrefix);
    auto partIter = nebula::value(partIterRet).get();
    while (partIter->valid()) {
      auto partHosts = MetaKeyUtils::parsePartVal(partIter->val());
      for (auto& h : partHosts) {
        if (std::find(hosts.begin(), hosts.end(), h) != hosts.end()) {
          LOG(INFO) << h << " is related with partition";
          code = nebula::cpp2::ErrorCode::E_RELATED_SPACE_EXISTS;
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
    LOG(INFO) << "List zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto zoneKey = iter->key();
    auto zoneName = MetaKeyUtils::parseZoneName(zoneKey);
    auto hs = MetaKeyUtils::parseZoneHosts(iter->val());
    // Delete all hosts in the zone
    if (std::all_of(hs.begin(), hs.end(), [&hosts](auto& h) {
          return std::find(hosts.begin(), hosts.end(), h) != hosts.end();
        })) {
      LOG(INFO) << "Drop zone " << zoneName;
      code = checkRelatedSpaceAndCollect(zoneName, &spaceMap);
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "Check related space failed";
        break;
      }

      holder->remove(MetaKeyUtils::zoneKey(zoneName));
    } else {
      // Delete some hosts in the zone
      for (auto& h : hosts) {
        auto it = std::find(hs.begin(), hs.end(), h);
        if (it != hs.end()) {
          hs.erase(it);
        }
      }

      auto zoneValue = MetaKeyUtils::zoneVal(hs);
      LOG(INFO) << "Zone Value: " << zoneValue;
      holder->put(MetaKeyUtils::zoneKey(zoneName), std::move(zoneValue));
    }
    CHECK_CODE_AND_BREAK();
    iter->next();
  }
  for (auto& [spaceId, properties] : spaceMap) {
    holder->put(MetaKeyUtils::spaceKey(spaceId), MetaKeyUtils::spaceVal(properties));
  }
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Detach the machine from cluster
  for (auto& host : hosts) {
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    auto ret = machineExist(machineKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "The machine " << host << " not existed!";
      code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
      break;
    }
    holder->remove(std::move(machineKey));

    auto hostKey = MetaKeyUtils::hostKey(host.host, host.port);
    ret = hostExist(hostKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "The host " << host << " not existed!";
    } else {
      holder->remove(std::move(hostKey));
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  auto batch = encodeBatchValue(std::move(holder)->getBatch());
  doBatchOperation(std::move(batch));
}

nebula::cpp2::ErrorCode DropHostsProcessor::checkRelatedSpaceAndCollect(
    const std::string& zoneName, std::map<GraphSpaceID, meta::cpp2::SpaceDesc>* spaceMap) {
  for (auto& [spaceId, properties] : *spaceMap) {
    const std::vector<std::string>& curZones = properties.get_zone_names();
    std::set<std::string> zm(curZones.begin(), curZones.end());
    if (zm.count(zoneName)) {
      int32_t zoneSize = zm.size();
      if (zoneSize == properties.get_replica_factor()) {
        LOG(INFO) << "Zone size is same with replica factor";
        return nebula::cpp2::ErrorCode::E_CONFLICT;
      } else {
        zm.erase(zoneName);
        std::vector<std::string> newZones(zm.begin(), zm.end());
        properties.zone_names_ref() = std::move(newZones);
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
