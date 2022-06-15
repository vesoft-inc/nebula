/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/RestoreProcessor.h"

#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode RestoreProcessor::replaceHostInPartition(
    kvstore::WriteBatch* batch, std::map<HostAddr, HostAddr>& hostMap) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto iterRet = doPrefix(spacePrefix, true);
  if (!nebula::ok(iterRet)) {
    retCode = nebula::error(iterRet);
    LOG(INFO) << "Space prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto iter = nebula::value(iterRet).get();

  std::vector<GraphSpaceID> allSpaceId;
  while (iter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(iter->key());
    allSpaceId.emplace_back(spaceId);
    iter->next();
  }
  LOG(INFO) << "allSpaceId.size()=" << allSpaceId.size();

  for (const auto& spaceId : allSpaceId) {
    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    auto iterPartRet = doPrefix(partPrefix, true);
    if (!nebula::ok(iterPartRet)) {
      retCode = nebula::error(iterPartRet);
      LOG(INFO) << "Part prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }
    iter = nebula::value(iterPartRet).get();

    while (iter->valid()) {
      bool needUpdate = false;
      auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
      for (auto& host : partHosts) {
        if (hostMap.find(host) != hostMap.end()) {
          host = hostMap[host];
          needUpdate = true;
        }
      }
      if (needUpdate) {
        batch->put(iter->key(), MetaKeyUtils::partVal(partHosts));
      }
      iter->next();
    }
  }

  return retCode;
}

nebula::cpp2::ErrorCode RestoreProcessor::replaceHostInZone(kvstore::WriteBatch* batch,
                                                            std::map<HostAddr, HostAddr>& hostMap) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
  const auto& zonePrefix = MetaKeyUtils::zonePrefix();
  auto iterRet = doPrefix(zonePrefix, true);
  if (!nebula::ok(iterRet)) {
    retCode = nebula::error(iterRet);
    LOG(INFO) << "Zone prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto iter = nebula::value(iterRet).get();

  // Explicit use of the zone feature is disabled, and a zone can only have one host.
  std::unordered_map<std::string, std::string> zoneNameMap;
  std::vector<kvstore::KV> zonekv;
  while (iter->valid()) {
    auto oldZoneKey = iter->key();
    auto oldZoneName = MetaKeyUtils::parseZoneName(oldZoneKey);
    auto hosts = MetaKeyUtils::parseZoneHosts(iter->val());

    CHECK_EQ(1, hosts.size());
    if (hostMap.find(hosts[0]) != hostMap.end()) {
      auto host = hostMap[hosts[0]];
      auto newZoneName = folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
      batch->remove(oldZoneKey);
      zonekv.emplace_back(
          std::make_pair(MetaKeyUtils::zoneKey(newZoneName), MetaKeyUtils::zoneVal({host})));
      zoneNameMap.emplace(oldZoneName, newZoneName);
    }
    iter->next();
  }

  for (auto& kv : zonekv) {
    batch->put(kv.first, kv.second);
  }

  // Update the zonename of spaceDesc in space system table
  std::map<GraphSpaceID, meta::cpp2::SpaceDesc> spaceMap;
  std::string spacePrefix = MetaKeyUtils::spacePrefix();
  auto spaceIterRet = doPrefix(spacePrefix, true);
  if (!nebula::ok(spaceIterRet)) {
    retCode = nebula::error(iterRet);
    LOG(INFO) << "Space prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto spaceIter = nebula::value(spaceIterRet).get();
  while (spaceIter->valid()) {
    spaceMap.emplace(MetaKeyUtils::spaceId(spaceIter->key()),
                     MetaKeyUtils::parseSpace(spaceIter->val()));
    spaceIter->next();
  }
  for (auto& [spaceId, properties] : spaceMap) {
    std::vector<std::string> curZones = properties.get_zone_names();
    for (size_t i = 0; i < curZones.size(); i++) {
      auto zoneName = curZones[i];
      if (zoneNameMap.find(zoneName) != zoneNameMap.end()) {
        curZones[i] = zoneNameMap[zoneName];
      }
    }
    properties.zone_names_ref() = std::move(curZones);
  }

  for (auto& [spaceId, properties] : spaceMap) {
    batch->put(MetaKeyUtils::spaceKey(spaceId), MetaKeyUtils::spaceVal(properties));
  }

  return retCode;
}

nebula::cpp2::ErrorCode RestoreProcessor::replaceHostInMachine(
    kvstore::WriteBatch* batch, std::map<HostAddr, HostAddr>& hostMap) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto retCode = nebula::cpp2::ErrorCode::SUCCEEDED;

  const auto& machinePrefix = MetaKeyUtils::machinePrefix();
  auto iterRet = doPrefix(machinePrefix, true);
  if (!nebula::ok(iterRet)) {
    retCode = nebula::error(iterRet);
    LOG(INFO) << "Machine prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto iter = nebula::value(iterRet).get();

  std::vector<std::string> newMachineKeys;
  while (iter->valid()) {
    auto machine = MetaKeyUtils::parseMachineKey(iter->key());
    if (hostMap.find(machine) != hostMap.end()) {
      batch->remove(MetaKeyUtils::machineKey(machine.host, machine.port));
      auto newMachineKey = MetaKeyUtils::machineKey(hostMap[machine].host, hostMap[machine].port);
      newMachineKeys.emplace_back(std::move(newMachineKey));
    }
    iter->next();
  }
  for (auto& newKey : newMachineKeys) {
    batch->put(newKey, "");
  }

  return retCode;
}

void RestoreProcessor::process(const cpp2::RestoreMetaReq& req) {
  auto files = req.get_files();
  if (files.empty()) {
    LOG(INFO) << "restore must contain the sst file.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_RESTORE_FAILURE);
    onFinished();
    return;
  }

  auto ret = kvstore_->restoreFromFiles(kDefaultSpaceId, files);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Failed to restore file";
    handleErrorCode(nebula::cpp2::ErrorCode::E_RESTORE_FAILURE);
    onFinished();
    return;
  }

  auto batch = kvstore_->startBatchWrite();
  auto replaceHosts = req.get_hosts();
  if (!replaceHosts.empty()) {
    std::map<HostAddr, HostAddr> hostMap;
    for (auto h : replaceHosts) {
      if (h.get_from_host() == h.get_to_host()) {
        continue;
      }

      hostMap[h.get_from_host()] = h.get_to_host();
    }

    if (!hostMap.empty()) {
      auto result = replaceHostInPartition(batch.get(), hostMap);
      if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "replaceHost in partition fails when recovered";
        handleErrorCode(result);
        onFinished();
        return;
      }

      result = replaceHostInZone(batch.get(), hostMap);
      if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "replaceHost in zone fails when recovered";
        handleErrorCode(result);
        onFinished();
        return;
      }

      result = replaceHostInMachine(batch.get(), hostMap);
      if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "replaceHost in machine fails when recovered";
        handleErrorCode(result);
        onFinished();
        return;
      }
    }
  }

  for (auto& f : files) {
    unlink(f.c_str());
  }

  ret = kvstore_->batchWriteWithoutReplicator(kDefaultSpaceId, std::move(batch));
  handleErrorCode(ret);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
