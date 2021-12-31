/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/CreateSpaceProcessor.h"

#include "meta/ActiveHostsMan.h"

DEFINE_int32(default_parts_num, 100, "The default number of parts when a space is created");
DEFINE_int32(default_replica_factor, 1, "The default replica factor when a space is created");

namespace nebula {
namespace meta {

void CreateSpaceProcessor::process(const cpp2::CreateSpaceReq& req) {
  folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
  auto properties = req.get_properties();
  auto spaceName = properties.get_space_name();
  auto spaceRet = getSpaceId(spaceName);

  if (nebula::ok(spaceRet)) {
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    if (!req.get_if_not_exists()) {
      LOG(ERROR) << "Create Space Failed : Space " << spaceName << " have existed!";
      ret = nebula::cpp2::ErrorCode::E_EXISTED;
    }
    resp_.id_ref() = to(nebula::value(spaceRet), EntryType::SPACE);
    handleErrorCode(ret);
    onFinished();
    return;
  } else {
    auto retCode = nebula::error(spaceRet);
    if (retCode != nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
      LOG(ERROR) << "Create Space Failed : Space  " << spaceName
                 << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
  }

  auto partitionNum = properties.get_partition_num();
  auto replicaFactor = properties.get_replica_factor();
  auto charsetName = properties.get_charset_name();
  auto collateName = properties.get_collate_name();
  auto& vid = properties.get_vid_type();
  auto vidSize = vid.type_length_ref().has_value() ? *vid.type_length_ref() : 0;
  auto vidType = vid.get_type();

  // Use default values or values from meta's configuration file
  if (partitionNum == 0) {
    partitionNum = FLAGS_default_parts_num;
    if (partitionNum <= 0) {
      LOG(ERROR) << "Create Space Failed : partition_num is illegal: " << partitionNum;
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    }
    // Set the default value back to the struct, which will be written to
    // storage
    properties.partition_num_ref() = partitionNum;
  }

  if (replicaFactor == 0) {
    replicaFactor = FLAGS_default_replica_factor;
    if (replicaFactor <= 0) {
      LOG(ERROR) << "Create Space Failed : replicaFactor is illegal: " << replicaFactor;
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    }
    // Set the default value back to the struct, which will be written to
    // storage
    properties.replica_factor_ref() = replicaFactor;
  }

  if (vidSize == 0) {
    LOG(ERROR) << "Create Space Failed : vid_size is illegal: " << vidSize;
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (vidType != nebula::cpp2::PropertyType::INT64 &&
      vidType != nebula::cpp2::PropertyType::FIXED_STRING) {
    LOG(ERROR) << "Create Space Failed : vid_type is illegal: "
               << apache::thrift::util::enumNameSafe(vidType);
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (vidType == nebula::cpp2::PropertyType::INT64 && vidSize != 8) {
    LOG(ERROR) << "Create Space Failed : vid_size should be 8 if vid type is integer: " << vidSize;
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  properties.vid_type_ref().value().type_length_ref() = vidSize;
  auto idRet = autoIncrementId();
  if (!nebula::ok(idRet)) {
    LOG(ERROR) << "Create Space Failed : Get space id failed";
    handleErrorCode(nebula::error(idRet));
    onFinished();
    return;
  }

  auto spaceId = nebula::value(idRet);
  std::vector<kvstore::KV> data;
  std::vector<::std::string> zones;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (properties.get_zone_names().empty()) {
    const auto& zonePrefix = MetaKeyUtils::zonePrefix();
    auto zoneIterRet = doPrefix(zonePrefix);
    if (!nebula::ok(zoneIterRet)) {
      code = nebula::error(zoneIterRet);
      LOG(ERROR) << "Get zones failed, error: " << apache::thrift::util::enumNameSafe(code);
      handleErrorCode(code);
      onFinished();
      return;
    }

    auto zoneIter = nebula::value(zoneIterRet).get();
    while (zoneIter->valid()) {
      auto zoneName = MetaKeyUtils::parseZoneName(zoneIter->key());
      zones.emplace_back(std::move(zoneName));
      zoneIter->next();
    }

    int32_t zoneNum = zones.size();
    if (replicaFactor > zoneNum) {
      LOG(ERROR) << "Replication number should less than or equal to zone number.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    }

    properties.zone_names_ref() = zones;
  } else {
    zones = properties.get_zone_names();
  }

  data.emplace_back(MetaKeyUtils::indexSpaceKey(spaceName),
                    std::string(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId)));
  data.emplace_back(MetaKeyUtils::spaceKey(spaceId), MetaKeyUtils::spaceVal(properties));
  for (auto& zone : zones) {
    auto zoneKey = MetaKeyUtils::zoneKey(zone);
    auto ret = doGet(zoneKey);
    if (!nebula::ok(ret)) {
      auto retCode = nebula::error(ret);
      if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        code = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
      }
      LOG(ERROR) << " Get Zone Name: " << zone << " failed.";
      break;
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Create space failed";
    handleErrorCode(code);
    onFinished();
    return;
  }

  int32_t zoneNum = zones.size();
  if (replicaFactor > zoneNum) {
    LOG(ERROR) << "Replication number should less than or equal to zone number.";
    LOG(ERROR) << "Replication number: " << replicaFactor << ", Zones size: " << zones.size();
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  auto hostLoadingRet = getHostLoading();
  if (!nebula::ok(hostLoadingRet)) {
    LOG(ERROR) << "Get host loading failed.";
    auto retCode = nebula::error(hostLoadingRet);
    if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      retCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  hostLoading_ = std::move(nebula::value(hostLoadingRet));
  std::unordered_map<std::string, Hosts> zoneHosts;
  for (auto& zone : zones) {
    auto zoneKey = MetaKeyUtils::zoneKey(zone);
    auto zoneValueRet = doGet(std::move(zoneKey));
    if (!nebula::ok(zoneValueRet)) {
      code = nebula::error(zoneValueRet);
      if (code == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        code = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
      }
      LOG(ERROR) << "Get zone " << zone << " failed.";
      break;
    }

    auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
    for (auto& host : hosts) {
      auto hostIter = hostLoading_.find(host);
      if (hostIter == hostLoading_.end()) {
        hostLoading_[host] = 0;
        zoneLoading_[zone] += 0;
      } else {
        zoneLoading_[zone] += hostIter->second;
      }
    }
    zoneHosts[zone] = std::move(hosts);
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Create space failed";
    handleErrorCode(code);
    onFinished();
    return;
  }

  for (auto partId = 1; partId <= partitionNum; partId++) {
    auto pickedZonesRet = pickLightLoadZones(replicaFactor);
    if (!pickedZonesRet.ok()) {
      LOG(ERROR) << "Pick zone failed.";
      code = nebula::cpp2::ErrorCode::E_INVALID_PARM;
      break;
    }

    auto pickedZones = std::move(pickedZonesRet).value();
    auto partHostsRet = pickHostsWithZone(pickedZones, zoneHosts);
    if (!partHostsRet.ok()) {
      LOG(ERROR) << "Pick hosts with zone failed.";
      code = nebula::cpp2::ErrorCode::E_INVALID_PARM;
      break;
    }

    auto partHosts = std::move(partHostsRet).value();
    if (partHosts.empty()) {
      LOG(ERROR) << "Pick hosts is empty.";
      code = nebula::cpp2::ErrorCode::E_INVALID_PARM;
      break;
    }

    std::stringstream ss;
    for (const auto& host : partHosts) {
      ss << host << ", ";
    }

    VLOG(3) << "Space " << spaceId << " part " << partId << " hosts " << ss.str();
    data.emplace_back(MetaKeyUtils::partKey(spaceId, partId), MetaKeyUtils::partVal(partHosts));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Create space failed";
    handleErrorCode(code);
    onFinished();
    return;
  }

  resp_.id_ref() = to(spaceId, EntryType::SPACE);
  doSyncPutAndUpdate(std::move(data));
  LOG(INFO) << "Create space " << spaceName;
}

ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<HostAddr, int32_t>>
CreateSpaceProcessor::getHostLoading() {
  const auto& prefix = MetaKeyUtils::partPrefix();
  auto iterRet = doPrefix(prefix);

  if (!nebula::ok(iterRet)) {
    LOG(ERROR) << "Prefix Parts Failed";
    return nebula::error(iterRet);
  }

  std::unordered_map<HostAddr, int32_t> result;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto hosts = MetaKeyUtils::parsePartVal(iter->val());
    for (auto& host : hosts) {
      result[host]++;
    }
    iter->next();
  }
  return result;
}

StatusOr<Hosts> CreateSpaceProcessor::pickHostsWithZone(
    const std::vector<std::string>& zones,
    const std::unordered_map<std::string, Hosts>& zoneHosts) {
  Hosts pickedHosts;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  for (auto iter = zoneHosts.begin(); iter != zoneHosts.end(); iter++) {
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      break;
    }

    if (iter->second.empty()) {
      LOG(ERROR) << "Zone " << iter->first << " is empty";
      code = nebula::cpp2::ErrorCode::E_INVALID_PARM;
      break;
    }

    auto zoneIter = std::find(std::begin(zones), std::end(zones), iter->first);
    if (zoneIter == std::end(zones)) {
      continue;
    }

    HostAddr picked;
    int32_t size = INT_MAX;
    for (auto& host : iter->second) {
      auto hostIter = hostLoading_.find(host);
      if (hostIter == hostLoading_.end()) {
        LOG(ERROR) << "Host " << host << " not found";
        code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
        break;
      }

      if (size > hostIter->second) {
        picked = host;
        size = hostIter->second;
      }
    }

    hostLoading_[picked] += 1;
    pickedHosts.emplace_back(toThriftHost(std::move(picked)));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("Host not found");
  }
  return pickedHosts;
}

StatusOr<std::vector<std::string>> CreateSpaceProcessor::pickLightLoadZones(int32_t replicaFactor) {
  std::multimap<int32_t, std::string> sortedMap;
  for (const auto& pair : zoneLoading_) {
    sortedMap.insert(std::make_pair(pair.second, pair.first));
  }
  std::vector<std::string> pickedZones;
  for (const auto& pair : sortedMap) {
    pickedZones.emplace_back(pair.second);
    zoneLoading_[pair.second] += 1;
    int32_t size = pickedZones.size();
    if (size == replicaFactor) {
      break;
    }
  }
  return pickedZones;
}

}  // namespace meta
}  // namespace nebula
