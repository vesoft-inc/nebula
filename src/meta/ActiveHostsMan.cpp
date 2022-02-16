/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/ActiveHostsMan.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <folly/synchronization/Baton.h>    // for Baton
#include <gflags/gflags.h>                  // for DECLARE_int32, DECLARE_ui...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>      // for find
#include <atomic>         // for atomic
#include <cstdint>        // for int64_t, uint32_t, int32_t
#include <memory>         // for unique_ptr, allocator_tra...
#include <new>            // for operator new
#include <ostream>        // for operator<<, basic_ostream
#include <tuple>          // for tie, ignore, tuple
#include <type_traits>    // for remove_reference<>::type
#include <unordered_set>  // for unordered_set, unordered_...

#include "common/datatypes/HostAddr.h"  // for HostAddr, operator<<, hash
#include "common/time/WallClock.h"      // for WallClock
#include "common/utils/MetaKeyUtils.h"  // for MetaKeyUtils, kDefaultPartId
#include "kvstore/Common.h"             // for KV
#include "kvstore/KVIterator.h"         // for KVIterator
#include "kvstore/KVStore.h"            // for KVStore
#include "meta/processors/Common.h"     // for LockUtils

DECLARE_int32(heartbeat_interval_secs);
DEFINE_int32(agent_heartbeat_interval_secs, 60, "Agent heartbeat interval in seconds");
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode ActiveHostsMan::updateHostInfo(kvstore::KVStore* kv,
                                                       const HostAddr& hostAddr,
                                                       const HostInfo& info,
                                                       const AllLeaders* allLeaders) {
  CHECK_NOTNULL(kv);
  std::vector<kvstore::KV> data;
  std::vector<std::string> leaderKeys;
  std::vector<int64_t> terms;
  if (allLeaders != nullptr) {
    for (auto& spaceLeaders : *allLeaders) {
      auto spaceId = spaceLeaders.first;
      for (auto& partLeader : spaceLeaders.second) {
        auto key = MetaKeyUtils::leaderKey(spaceId, partLeader.get_part_id());
        leaderKeys.emplace_back(std::move(key));
        terms.emplace_back(partLeader.get_term());
      }
    }
    auto keys = leaderKeys;
    std::vector<std::string> vals;
    auto [rc, statusVec] = kv->multiGet(kDefaultSpaceId, kDefaultPartId, std::move(keys), &vals);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED &&
        rc != nebula::cpp2::ErrorCode::E_PARTIAL_RESULT) {
      LOG(INFO) << "error rc = " << apache::thrift::util::enumNameSafe(rc);
      return rc;
    }
    TermID term = -1;
    nebula::cpp2::ErrorCode code;
    for (auto i = 0U; i != leaderKeys.size(); ++i) {
      if (statusVec[i].ok()) {
        std::tie(std::ignore, term, code) = MetaKeyUtils::parseLeaderValV3(vals[i]);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          LOG(INFO) << apache::thrift::util::enumNameSafe(code);
          continue;
        }
        if (terms[i] <= term) {
          continue;
        }
      }
      // write directly if not exist, or update if has greater term
      auto val = MetaKeyUtils::leaderValV3(hostAddr, terms[i]);
      data.emplace_back(std::make_pair(leaderKeys[i], std::move(val)));
    }
  }
  // indicate whether any leader info is updated
  bool hasUpdate = !data.empty();
  data.emplace_back(MetaKeyUtils::hostKey(hostAddr.host, hostAddr.port), HostInfo::encodeV2(info));

  folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
  folly::Baton<true, std::atomic> baton;
  nebula::cpp2::ErrorCode ret;
  kv->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = code;
        baton.post();
      });
  baton.wait();
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }
  if (hasUpdate) {
    ret = LastUpdateTimeMan::update(kv, time::WallClock::fastNowInMilliSec());
  }
  return ret;
}

bool ActiveHostsMan::machineRegisted(kvstore::KVStore* kv, const HostAddr& hostAddr) {
  auto machineKey = MetaKeyUtils::machineKey(hostAddr.host, hostAddr.port);
  std::string machineValue;
  auto code = kv->get(kDefaultSpaceId, kDefaultPartId, machineKey, &machineValue);
  return code == nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::pair<HostAddr, cpp2::HostRole>>>
ActiveHostsMan::getServicesInHost(kvstore::KVStore* kv, const std::string& hostname) {
  const auto& prefix = MetaKeyUtils::hostPrefix();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Failed to get services in the host: " << hostname << ", error "
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::vector<std::pair<HostAddr, cpp2::HostRole>> hosts;
  while (iter->valid()) {
    auto addr = MetaKeyUtils::parseHostKey(iter->key());
    HostInfo info = HostInfo::decode(iter->val());

    if (addr.host == hostname) {
      hosts.emplace_back(addr, info.role_);
    }
    iter->next();
  }

  return hosts;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> ActiveHostsMan::getActiveHosts(
    kvstore::KVStore* kv, int32_t expiredTTL, cpp2::HostRole role) {
  const auto& machinePrefix = MetaKeyUtils::machinePrefix();
  std::unique_ptr<kvstore::KVIterator> machineIter;
  auto retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, machinePrefix, &machineIter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Failed to get machines, error " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::unordered_set<HostAddr> machines;
  while (machineIter->valid()) {
    auto machine = MetaKeyUtils::parseMachineKey(machineIter->key());
    machines.emplace(std::move(machine));
    machineIter->next();
  }

  const auto& prefix = MetaKeyUtils::hostPrefix();
  std::unique_ptr<kvstore::KVIterator> iter;
  retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Failed to get active hosts, error "
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::vector<HostAddr> hosts;
  int64_t expiredTime =
      FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor;  // meta/storage/graph
  if (role == cpp2::HostRole::AGENT) {
    expiredTime = FLAGS_agent_heartbeat_interval_secs * FLAGS_expired_time_factor;
  }
  int64_t threshold = (expiredTTL == 0 ? expiredTime : expiredTTL) * 1000;
  auto now = time::WallClock::fastNowInMilliSec();
  while (iter->valid()) {
    auto host = MetaKeyUtils::parseHostKey(iter->key());
    HostInfo info = HostInfo::decode(iter->val());

    if (info.role_ == cpp2::HostRole::STORAGE &&
        std::find(machines.begin(), machines.end(), host) == machines.end()) {
      retCode = nebula::cpp2::ErrorCode::E_MACHINE_NOT_FOUND;
      LOG(INFO) << "Machine not found " << host;
      break;
    }

    if (info.role_ == role) {
      if (now - info.lastHBTimeInMilliSec_ < threshold) {
        hosts.emplace_back(host.host, host.port);
      }
    }
    iter->next();
  }

  return hosts;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> ActiveHostsMan::getActiveHostsInZone(
    kvstore::KVStore* kv, const std::string& zoneName, int32_t expiredTTL) {
  std::vector<HostAddr> activeHosts;
  std::string zoneValue;
  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get zone " << zoneName
              << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto hosts = MetaKeyUtils::parseZoneHosts(std::move(zoneValue));
  auto now = time::WallClock::fastNowInMilliSec();
  int64_t threshold =
      (expiredTTL == 0 ? FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor : expiredTTL) *
      1000;
  for (auto& host : hosts) {
    auto infoRet = getHostInfo(kv, host);
    if (!nebula::ok(infoRet)) {
      return nebula::error(infoRet);
    }

    auto info = nebula::value(infoRet);
    if (now - info.lastHBTimeInMilliSec_ < threshold) {
      activeHosts.emplace_back(host.host, host.port);
    }
  }
  return activeHosts;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> ActiveHostsMan::getActiveHostsWithZones(
    kvstore::KVStore* kv, GraphSpaceID spaceId, int32_t expiredTTL) {
  std::string spaceValue;
  std::vector<HostAddr> activeHosts;
  auto spaceKey = MetaKeyUtils::spaceKey(spaceId);
  auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceValue);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get space failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::string groupValue;
  auto space = MetaKeyUtils::parseSpace(std::move(spaceValue));
  auto zoneNames = *space.zone_names_ref();
  for (const auto& zoneName : zoneNames) {
    auto hostsRet = getActiveHostsInZone(kv, zoneName, expiredTTL);
    if (!nebula::ok(hostsRet)) {
      return nebula::error(hostsRet);
    }
    auto hosts = nebula::value(hostsRet);
    activeHosts.insert(activeHosts.end(), hosts.begin(), hosts.end());
  }
  return activeHosts;
}

ErrorOr<nebula::cpp2::ErrorCode, bool> ActiveHostsMan::isLived(kvstore::KVStore* kv,
                                                               const HostAddr& host) {
  auto activeHostsRet = getActiveHosts(kv);
  if (!nebula::ok(activeHostsRet)) {
    return nebula::error(activeHostsRet);
  }
  auto activeHosts = nebula::value(activeHostsRet);

  return std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end();
}

ErrorOr<nebula::cpp2::ErrorCode, HostInfo> ActiveHostsMan::getHostInfo(kvstore::KVStore* kv,
                                                                       const HostAddr& host) {
  auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
  std::string machineValue;
  auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, machineKey, &machineValue);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get machine info " << host
              << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto hostKey = MetaKeyUtils::hostKey(host.host, host.port);
  std::string hostValue;
  retCode = kv->get(kDefaultSpaceId, kDefaultPartId, hostKey, &hostValue);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get host info " << host
              << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  return HostInfo::decode(hostValue);
}

nebula::cpp2::ErrorCode LastUpdateTimeMan::update(kvstore::KVStore* kv,
                                                  const int64_t timeInMilliSec) {
  CHECK_NOTNULL(kv);
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::lastUpdateTimeKey(),
                    MetaKeyUtils::lastUpdateTimeVal(timeInMilliSec));

  folly::SharedMutex::WriteHolder wHolder(LockUtils::lastUpdateTimeLock());
  folly::Baton<true, std::atomic> baton;
  nebula::cpp2::ErrorCode ret;
  kv->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = code;
        baton.post();
      });
  baton.wait();
  return ret;
}

ErrorOr<nebula::cpp2::ErrorCode, int64_t> LastUpdateTimeMan::get(kvstore::KVStore* kv) {
  CHECK_NOTNULL(kv);
  auto key = MetaKeyUtils::lastUpdateTimeKey();
  std::string val;
  auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get last update time failed, error: "
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  return *reinterpret_cast<const int64_t*>(val.data());
}

}  // namespace meta
}  // namespace nebula
