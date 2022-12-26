/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/CreateSpaceProcessor.h"

#include "meta/ActiveHostsMan.h"

DEFINE_int32(default_parts_num, 100, "The default number of parts when a space is created");
DEFINE_int32(default_replica_factor, 1, "The default replica factor when a space is created");
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

void CreateSpaceProcessor::process(const cpp2::CreateSpaceReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto properties = req.get_properties();
  auto spaceName = properties.get_space_name();
  auto spaceRet = getSpaceId(spaceName);

  if (nebula::ok(spaceRet)) {
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    if (!req.get_if_not_exists()) {
      LOG(INFO) << "Create Space Failed : Space " << spaceName << " have existed!";
      ret = nebula::cpp2::ErrorCode::E_EXISTED;
    }
    resp_.id_ref() = to(nebula::value(spaceRet), EntryType::SPACE);
    handleErrorCode(ret);
    onFinished();
    return;
  } else {
    auto retCode = nebula::error(spaceRet);
    if (retCode != nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
      LOG(INFO) << "Create Space Failed : Space  " << spaceName
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
      LOG(INFO) << "Create Space Failed : partition_num is illegal: " << partitionNum;
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
      LOG(INFO) << "Create Space Failed : replicaFactor is illegal: " << replicaFactor;
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    }
    // Set the default value back to the struct, which will be written to
    // storage
    properties.replica_factor_ref() = replicaFactor;
  }

  if (vidSize == 0) {
    LOG(INFO) << "Create Space Failed : vid_size is illegal: " << vidSize;
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (vidType != nebula::cpp2::PropertyType::INT64 &&
      vidType != nebula::cpp2::PropertyType::FIXED_STRING) {
    LOG(INFO) << "Create Space Failed : vid_type is illegal: "
              << apache::thrift::util::enumNameSafe(vidType);
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (vidType == nebula::cpp2::PropertyType::INT64 && vidSize != 8) {
    LOG(INFO) << "Create Space Failed : vid_size should be 8 if vid type is integer: " << vidSize;
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  properties.vid_type_ref().value().type_length_ref() = vidSize;
  auto idRet = autoIncrementId();
  if (!nebula::ok(idRet)) {
    LOG(INFO) << "Create Space Failed : Get space id failed";
    handleErrorCode(nebula::error(idRet));
    onFinished();
    return;
  }

  auto spaceId = nebula::value(idRet);
  std::vector<kvstore::KV> data;
  std::vector<::std::string> zones;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (properties.get_zone_names().empty()) {
    // If zone names is empty, then this space could use all zones.
    const auto& zonePrefix = MetaKeyUtils::zonePrefix();
    auto zoneIterRet = doPrefix(zonePrefix);
    if (!nebula::ok(zoneIterRet)) {
      code = nebula::error(zoneIterRet);
      LOG(INFO) << "Get zones failed, error: " << apache::thrift::util::enumNameSafe(code);
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

    properties.zone_names_ref() = zones;
  } else {
    zones = properties.get_zone_names();
  }

  auto it = std::unique(zones.begin(), zones.end());
  if (it != zones.end()) {
    LOG(INFO) << "Zones have duplicated.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  int32_t zoneNum = zones.size();
  if (replicaFactor > zoneNum) {
    LOG(INFO) << "Replication number should less than or equal to zone number.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_ENOUGH);
    onFinished();
    return;
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
      LOG(INFO) << " Get Zone Name: " << zone << " failed.";
      break;
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Create space failed";
    handleErrorCode(code);
    onFinished();
    return;
  }

  int32_t activeZoneSize = 0;
  std::unordered_map<std::string, Hosts> zoneHosts;
  for (auto& zone : zones) {
    auto zoneKey = MetaKeyUtils::zoneKey(zone);
    auto zoneValueRet = doGet(std::move(zoneKey));
    if (!nebula::ok(zoneValueRet)) {
      code = nebula::error(zoneValueRet);
      if (code == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        code = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
      }
      LOG(INFO) << "Get zone " << zone << " failed.";
      break;
    }

    auto now = time::WallClock::fastNowInMilliSec();
    auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
    for (auto& host : hosts) {
      auto key = MetaKeyUtils::hostKey(host.host, host.port);
      auto ret = doGet(key);
      if (!nebula::ok(ret)) {
        LOG(ERROR) << "Get host " << host << " failed.";
        continue;
      }

      HostInfo info = HostInfo::decode(nebula::value(ret));
      if (now - info.lastHBTimeInMilliSec_ <
          FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor * 1000) {
        activeZoneSize += 1;
        auto hostIter = hostLoading_.find(host);
        if (hostIter == hostLoading_.end()) {
          hostLoading_[host] = 0;
          zoneLoading_[zone] += 0;
        } else {
          zoneLoading_[zone] += hostIter->second;
        }
      } else {
        LOG(WARNING) << "Host " << host << " expired";
      }
    }

    CHECK_CODE_AND_BREAK();
    zoneHosts[zone] = std::move(hosts);
  }

  if (replicaFactor > activeZoneSize) {
    LOG(ERROR) << "Replication number should less than or equal to active zone number.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_ENOUGH);
    onFinished();
    return;
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Create space failed";
    handleErrorCode(code);
    onFinished();
    return;
  }

  for (auto partId = 1; partId <= partitionNum; partId++) {
    auto pickedZonesRet = pickLightLoadZones(replicaFactor);
    if (!pickedZonesRet.ok()) {
      LOG(INFO) << "Pick zone failed.";
      code = nebula::cpp2::ErrorCode::E_INVALID_PARM;
      break;
    }

    auto pickedZones = std::move(pickedZonesRet).value();
    auto partHostsRet = pickHostsWithZone(pickedZones, zoneHosts);
    if (!nebula::ok(partHostsRet)) {
      LOG(INFO) << "Pick hosts with zone failed.";
      code = nebula::error(partHostsRet);
      break;
    }

    auto partHosts = nebula::value(partHostsRet);
    if (partHosts.empty()) {
      LOG(INFO) << "Pick hosts is empty.";
      code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
      break;
    }

    std::stringstream ss;
    for (const auto& host : partHosts) {
      ss << host << ", ";
    }

    VLOG(2) << "Space " << spaceId << " part " << partId << " hosts " << ss.str();
    data.emplace_back(MetaKeyUtils::partKey(spaceId, partId), MetaKeyUtils::partVal(partHosts));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Create space failed";
    handleErrorCode(code);
    onFinished();
    return;
  }

  resp_.id_ref() = to(spaceId, EntryType::SPACE);
  LOG(INFO) << "Create space " << spaceName;
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

ErrorOr<nebula::cpp2::ErrorCode, Hosts> CreateSpaceProcessor::pickHostsWithZone(
    const std::vector<std::string>& zones,
    const std::unordered_map<std::string, Hosts>& zoneHosts) {
  Hosts pickedHosts;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  for (auto iter = zoneHosts.begin(); iter != zoneHosts.end(); iter++) {
    if (iter->second.empty()) {
      LOG(INFO) << "Zone " << iter->first << " is empty";
      code = nebula::cpp2::ErrorCode::E_ZONE_IS_EMPTY;
      break;
    }

    auto zoneIter = std::find(std::begin(zones), std::end(zones), iter->first);
    if (zoneIter == std::end(zones)) {
      continue;
    }

    // pick the host with the least load
    HostAddr picked;
    int32_t size = INT_MAX;
    for (auto& host : iter->second) {
      auto hostIter = hostLoading_.find(host);
      if (hostIter == hostLoading_.end()) {
        LOG(INFO) << "Host " << host << " not found";
        code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
        break;
      }

      if (size > hostIter->second) {
        picked = host;
        size = hostIter->second;
      }
    }

    CHECK_CODE_AND_BREAK();
    hostLoading_[picked] += 1;
    pickedHosts.emplace_back(toThriftHost(std::move(picked)));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
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
