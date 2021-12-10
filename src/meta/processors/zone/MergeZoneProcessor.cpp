/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/MergeZoneProcessor.h"

namespace nebula {
namespace meta {

void MergeZoneProcessor::process(const cpp2::MergeZoneReq& req) {
  auto zones = req.get_zones();

  // Confirm that the parameter is not empty.
  if (zones.empty()) {
    LOG(ERROR) << "Zones is empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (zones.size() == 1) {
    LOG(ERROR) << "Only one zone is no need to merge";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  // Confirm that there are no duplicates in the parameters.
  if (std::unique(zones.begin(), zones.end()) != zones.end()) {
    LOG(ERROR) << "Zones have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  auto zoneName = req.get_zone_name();
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  // Ensure that all zone to be merged exist
  for (auto& zone : zones) {
    auto zoneKey = MetaKeyUtils::zoneKey(zone);
    auto zoneValueRet = doGet(std::move(zoneKey));
    if (!nebula::ok(zoneValueRet)) {
      code = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
      LOG(ERROR) << "Zone " << zone << " not existed";
      break;
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // For each graph space, check whether there are conflicts
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(spacePrefix);
  if (!nebula::ok(ret)) {
    code = nebula::error(ret);
    LOG(ERROR) << "List spaces failed, error " << apache::thrift::util::enumNameSafe(code);
    handleErrorCode(code);
    onFinished();
    return;
  }

  auto iter = nebula::value(ret).get();
  std::sort(zones.begin(), zones.end());
  while (iter->valid()) {
    auto spaceKey = iter->key();
    auto spaceId = MetaKeyUtils::spaceId(spaceKey);
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    auto spaceZones = properties.get_zone_names();
    size_t replicaFactor = properties.get_replica_factor();

    std::sort(spaceZones.begin(), spaceZones.end());
    std::vector<std::string> intersectionZones;
    std::set_intersection(spaceZones.begin(),
                          spaceZones.end(),
                          zones.begin(),
                          zones.end(),
                          std::back_inserter(intersectionZones));

    if (spaceZones.size() - intersectionZones.size() + 1 < replicaFactor) {
      LOG(ERROR) << "Merge Zone will cause replica number not enough";
      code = nebula::cpp2::ErrorCode::E_INVALID_PARM;
      break;
    }

    auto hostPartsRet = assembleHostParts(spaceId);
    if (!nebula::ok(hostPartsRet)) {
      code = nebula::error(hostPartsRet);
      LOG(ERROR) << "Assemble host parts failed: " << apache::thrift::util::enumNameSafe(code);
      break;
    }

    auto hostParts = nebula::value(hostPartsRet);
    std::vector<PartitionID> totalParts;
    for (auto& zone : intersectionZones) {
      auto zoneKey = MetaKeyUtils::zoneKey(zone);
      auto zoneValueRet = doGet(std::move(zoneKey));
      auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
      for (auto& host : hosts) {
        auto hp = hostParts.find(host);
        if (hp == hostParts.end()) {
          LOG(ERROR) << "Host " << host << " not found";
          code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
          break;
        }

        auto& parts = hp->second;
        for (auto part : parts) {
          auto it = std::find(totalParts.begin(), totalParts.end(), part);
          if (it != totalParts.end()) {
            LOG(ERROR) << "Part " << part << " have exist";
            code = nebula::cpp2::ErrorCode::E_CONFLICT;
            break;
          }
        }

        CHECK_CODE_AND_BREAK();
        totalParts.insert(totalParts.end(), parts.begin(), parts.end());
      }
      CHECK_CODE_AND_BREAK();
    }
    CHECK_CODE_AND_BREAK();
    iter->next();
  }  // space

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Check parts failed, error " << apache::thrift::util::enumNameSafe(code);
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Rewrite space properties
  ret = doPrefix(spacePrefix);
  iter = nebula::value(ret).get();
  std::vector<kvstore::KV> data;
  while (iter->valid()) {
    auto spaceKey = iter->key();
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    auto spaceZones = properties.get_zone_names();
    bool replacement = false;
    for (auto& zone : zones) {
      auto it = std::find(spaceZones.begin(), spaceZones.end(), zone);
      if (it != spaceZones.end()) {
        LOG(INFO) << "REMOVE ZONE " << zone;
        replacement = true;
        spaceZones.erase(it);
      }
    }

    if (replacement) {
      spaceZones.emplace_back(zoneName);
      properties.set_zone_names(std::move(spaceZones));
      auto spaceVal = MetaKeyUtils::spaceVal(properties);
      data.emplace_back(std::move(spaceKey), std::move(spaceVal));
    }
    iter->next();
  }

  // Write the merged zone into meta service
  auto key = MetaKeyUtils::zoneKey(zoneName);
  std::vector<HostAddr> zoneHosts;
  for (auto& zone : zones) {
    auto zoneKey = MetaKeyUtils::zoneKey(zone);
    auto zoneValueRet = doGet(std::move(zoneKey));
    auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
    zoneHosts.insert(zoneHosts.end(), hosts.begin(), hosts.end());
  }
  auto zoneVal = MetaKeyUtils::zoneVal(std::move(zoneHosts));

  // Remove original zones
  LOG(INFO) << "Remove original zones size " << zones.size();
  std::vector<std::string> zoneKeys;
  for (auto& zone : zones) {
    LOG(INFO) << "Remove Zone " << zone;
    zoneKeys.emplace_back(MetaKeyUtils::zoneKey(zone));
  }
  folly::Baton<true, std::atomic> baton;
  auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiRemove(kDefaultSpaceId,
                             kDefaultPartId,
                             std::move(zoneKeys),
                             [&result, &baton](nebula::cpp2::ErrorCode res) {
                               if (nebula::cpp2::ErrorCode::SUCCEEDED != res) {
                                 result = res;
                                 LOG(ERROR) << "Remove data error on meta server";
                               }
                               baton.post();
                             });
  baton.wait();
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    this->handleErrorCode(result);
    this->onFinished();
    return;
  }

  data.emplace_back(std::move(key), std::move(zoneVal));
  doSyncPutAndUpdate(std::move(data));
}

ErrorOr<nebula::cpp2::ErrorCode, HostParts> MergeZoneProcessor::assembleHostParts(
    GraphSpaceID spaceId) {
  std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  if (!nebula::ok(activeHostsRet)) {
    LOG(ERROR) << "Get active hosts failed";
    return nebula::error(activeHostsRet);
  }

  auto activeHosts = nebula::value(activeHostsRet);
  for (auto& host : activeHosts) {
    hostParts[host] = std::vector<PartitionID>();
  }

  std::unique_ptr<kvstore::KVIterator> iter;
  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId << " "
               << apache::thrift::util::enumNameSafe(code);
    return code;
  }

  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
    for (auto& ph : partHosts) {
      hostParts[ph].emplace_back(partId);
    }
    iter->next();
  }
  return hostParts;
}

}  // namespace meta
}  // namespace nebula
