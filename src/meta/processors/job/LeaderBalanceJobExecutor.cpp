/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/LeaderBalanceJobExecutor.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "common/utils/MetaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobUtils.h"

DEFINE_double(leader_balance_deviation,
              0.05,
              "after leader balance, leader count should in range "
              "[avg * (1 - deviation), avg * (1 + deviation)]");

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode LeaderBalanceJobExecutor::getAllSpaces(
    std::vector<std::tuple<GraphSpaceID, int32_t, bool>>& spaces) {
  // Get all spaces
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& prefix = MetaKeyUtils::spacePrefix();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get all spaces failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  while (iter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(iter->key());
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    bool zoned = !properties.get_zone_names().empty();
    spaces.emplace_back(spaceId, *properties.replica_factor_ref(), zoned);
    iter->next();
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, bool> LeaderBalanceJobExecutor::getHostParts(GraphSpaceID spaceId,
                                                                              bool dependentOnZone,
                                                                              HostParts& hostParts,
                                                                              int32_t& totalParts) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Access kvstore failed, spaceId " << spaceId << " "
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
    for (auto& ph : partHosts) {
      hostParts[ph].emplace_back(partId);
    }
    totalParts++;
    iter->next();
  }

  LOG(INFO) << "Host size: " << hostParts.size();
  auto key = MetaKeyUtils::spaceKey(spaceId);
  std::string value;
  retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Access kvstore failed, spaceId " << spaceId
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto properties = MetaKeyUtils::parseSpace(value);
  if (totalParts != properties.get_partition_num()) {
    LOG(INFO) << "Partition number not equals " << totalParts << " : "
              << properties.get_partition_num();
    return false;
  }

  int32_t replica = properties.get_replica_factor();
  LOG(INFO) << "Replica " << replica;
  if (dependentOnZone && !properties.get_zone_names().empty()) {
    auto zoneNames = properties.get_zone_names();
    int32_t zoneSize = zoneNames.size();
    LOG(INFO) << "Zone Size " << zoneSize;
    auto activeHostsRet = ActiveHostsMan::getActiveHostsWithZones(kvstore_, spaceId);
    if (!nebula::ok(activeHostsRet)) {
      return nebula::error(activeHostsRet);
    }

    std::vector<HostAddr> expand;
    auto activeHosts = nebula::value(activeHostsRet);
    std::vector<HostAddr> lostHosts;
    calDiff(hostParts, activeHosts, expand, lostHosts);
    // confirmedHostParts is new part allocation map after balance, it would include newlyAdded
    // and exclude lostHosts
    HostParts confirmedHostParts(hostParts);
    for (const auto& h : expand) {
      LOG(INFO) << "Found new host " << h;
      confirmedHostParts.emplace(h, std::vector<PartitionID>());
    }
    for (const auto& h : lostHosts) {
      LOG(INFO) << "Lost host " << h;
      confirmedHostParts.erase(h);
    }

    auto zonePartsRet = assembleZoneParts(zoneNames, confirmedHostParts);
    if (zonePartsRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Assemble Zone Parts failed";
      return zonePartsRet;
    }
  }

  totalParts *= replica;
  return true;
}

nebula::cpp2::ErrorCode LeaderBalanceJobExecutor::assembleZoneParts(
    const std::vector<std::string>& zoneNames, HostParts& hostParts) {
  // zoneHosts use to record this host belong to zone's hosts
  std::unordered_map<std::pair<HostAddr, std::string>, std::vector<HostAddr>> zoneHosts;
  for (const auto& zoneName : zoneNames) {
    LOG(INFO) << "Zone Name: " << zoneName;
    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    std::string zoneValue;
    auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Get zone " << zoneName
                << " failed: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    auto hosts = MetaKeyUtils::parseZoneHosts(std::move(zoneValue));
    for (const auto& host : hosts) {
      LOG(INFO) << "Host for zone " << host;
      auto pair = std::pair<HostAddr, std::string>(std::move(host), zoneName);
      auto& hs = zoneHosts[std::move(pair)];
      hs.insert(hs.end(), hosts.begin(), hosts.end());
    }
  }

  for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
    auto host = it->first;
    LOG(INFO) << "Host: " << host;
    auto zoneIter =
        std::find_if(zoneHosts.begin(), zoneHosts.end(), [host](const auto& pair) -> bool {
          return host == pair.first.first;
        });

    if (zoneIter == zoneHosts.end()) {
      LOG(INFO) << it->first << " have lost";
      continue;
    }

    auto& hosts = zoneIter->second;
    auto name = zoneIter->first.second;
    zoneHosts_[name] = hosts;
    for (auto hostIter = hosts.begin(); hostIter != hosts.end(); hostIter++) {
      auto partIter = hostParts.find(*hostIter);
      LOG(INFO) << "Zone " << name << " have the host " << it->first;
      if (partIter == hostParts.end()) {
        zoneParts_[it->first] = ZoneNameAndParts(name, std::vector<PartitionID>());
      } else {
        zoneParts_[it->first] = ZoneNameAndParts(name, partIter->second);
      }
    }
  }

  for (auto it = zoneHosts.begin(); it != zoneHosts.end(); it++) {
    auto host = it->first.first;
    auto& hosts = it->second;
    for (auto hostIter = hosts.begin(); hostIter != hosts.end(); hostIter++) {
      auto h = *hostIter;
      auto iter = std::find_if(hostParts.begin(), hostParts.end(), [h](const auto& pair) -> bool {
        return h == pair.first;
      });

      if (iter == hostParts.end()) {
        continue;
      }

      auto& parts = iter->second;
      auto& hp = relatedParts_[host];
      hp.insert(hp.end(), parts.begin(), parts.end());
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void LeaderBalanceJobExecutor::calDiff(const HostParts& hostParts,
                                       const std::vector<HostAddr>& activeHosts,
                                       std::vector<HostAddr>& expand,
                                       std::vector<HostAddr>& lost) {
  for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
    LOG(INFO) << "Original Host " << it->first << ", parts " << it->second.size();
    if (std::find(activeHosts.begin(), activeHosts.end(), it->first) == activeHosts.end() &&
        std::find(lost.begin(), lost.end(), it->first) == lost.end()) {
      lost.emplace_back(it->first);
    }
  }
  for (auto& h : activeHosts) {
    LOG(INFO) << "Active host " << h;
    if (hostParts.find(h) == hostParts.end()) {
      expand.emplace_back(h);
    }
  }
}

LeaderBalanceJobExecutor::LeaderBalanceJobExecutor(JobID jobId,
                                                   kvstore::KVStore* kvstore,
                                                   AdminClient* adminClient,
                                                   const std::vector<std::string>& params)
    : MetaJobExecutor(jobId, kvstore, adminClient, params),
      inLeaderBalance_(false),
      hostLeaderMap_(nullptr) {
  executor_.reset(new folly::CPUThreadPoolExecutor(1));
}

nebula::cpp2::ErrorCode LeaderBalanceJobExecutor::finish(bool) {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status> LeaderBalanceJobExecutor::executeInternal() {
  folly::Promise<Status> promise;
  auto future = promise.getFuture();
  // Space ID, Replica Factor and Dependent On Group
  std::vector<std::tuple<GraphSpaceID, int32_t, bool>> spaces;
  auto ret = getAllSpaces(spaces);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      ret = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }
    return Status::Error("Can't get spaces");
  }

  bool expected = false;
  if (inLeaderBalance_.compare_exchange_strong(expected, true)) {
    hostLeaderMap_.reset(new HostLeaderMap);
    auto status = adminClient_->getLeaderDist(hostLeaderMap_.get()).get();
    if (!status.ok() || hostLeaderMap_->empty()) {
      inLeaderBalance_ = false;
      return Status::Error("Get leader distribution failed");
    }

    std::vector<folly::SemiFuture<Status>> futures;
    for (const auto& spaceInfo : spaces) {
      auto spaceId = std::get<0>(spaceInfo);
      auto replicaFactor = std::get<1>(spaceInfo);
      auto dependentOnZone = std::get<2>(spaceInfo);
      LeaderBalancePlan plan;
      auto balanceResult = buildLeaderBalancePlan(
          hostLeaderMap_.get(), spaceId, replicaFactor, dependentOnZone, plan);
      if (!nebula::ok(balanceResult) || !nebula::value(balanceResult)) {
        LOG(INFO) << "Building leader balance plan failed "
                  << "Space: " << spaceId;
        continue;
      }
      simplifyLeaderBalancePlan(spaceId, plan);
      for (const auto& task : plan) {
        futures.emplace_back(adminClient_->transLeader(std::get<0>(task),
                                                       std::get<1>(task),
                                                       std::move(std::get<2>(task)),
                                                       std::move(std::get<3>(task))));
      }
    }

    int32_t failed = 0;
    folly::collectAll(futures)
        .via(executor_.get())
        .thenTry([&](const auto& result) {
          auto tries = result.value();
          for (const auto& t : tries) {
            if (!t.value().ok()) {
              ++failed;
            }
          }
        })
        .wait();

    inLeaderBalance_ = false;
    if (failed != 0) {
      return Status::Error("partiton failed to transfer leader");
    }
    executorOnFinished_(meta::cpp2::JobStatus::FINISHED);
    return Status::OK();
  }
  executorOnFinished_(meta::cpp2::JobStatus::FINISHED);
  return Status::OK();
}

ErrorOr<nebula::cpp2::ErrorCode, bool> LeaderBalanceJobExecutor::buildLeaderBalancePlan(
    HostLeaderMap* hostLeaderMap,
    GraphSpaceID spaceId,
    int32_t replicaFactor,
    bool dependentOnZone,
    LeaderBalancePlan& plan,
    bool useDeviation) {
  PartAllocation peersMap;
  HostParts leaderHostParts;
  size_t leaderParts = 0;
  // store peers of all paritions in peerMap
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Access kvstore failed, spaceId " << spaceId << static_cast<int32_t>(retCode);
    return retCode;
  }

  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    auto peers = MetaKeyUtils::parsePartVal(iter->val());
    peersMap[partId] = std::move(peers);
    ++leaderParts;
    iter->next();
  }

  int32_t totalParts = 0;
  HostParts allHostParts;
  auto result = getHostParts(spaceId, dependentOnZone, allHostParts, totalParts);
  if (!nebula::ok(result)) {
    return nebula::error(result);
  } else {
    auto retVal = nebula::value(result);
    if (!retVal || totalParts == 0 || allHostParts.empty()) {
      LOG(INFO) << "Invalid space " << spaceId;
      return false;
    }
  }

  std::unordered_set<HostAddr> activeHosts;
  for (const auto& host : *hostLeaderMap) {
    // only balance leader between hosts which have valid partition
    if (!allHostParts[host.first].empty()) {
      activeHosts.emplace(host.first);
      leaderHostParts[host.first] = (*hostLeaderMap)[host.first][spaceId];
    }
  }

  if (activeHosts.empty()) {
    LOG(INFO) << "No active hosts";
    return false;
  }

  if (dependentOnZone) {
    for (auto it = allHostParts.begin(); it != allHostParts.end(); it++) {
      auto min = it->second.size() / replicaFactor;
      LOG(INFO) << "Host: " << it->first << " Bounds: " << min << " : " << min + 1;
      hostBounds_[it->first] = std::make_pair(min, min + 1);
    }
  } else {
    size_t activeSize = activeHosts.size();
    size_t globalAvg = leaderParts / activeSize;
    size_t globalMin = globalAvg;
    size_t globalMax = globalAvg;
    if (leaderParts % activeSize != 0) {
      globalMax += 1;
    }

    if (useDeviation) {
      globalMin = std::ceil(static_cast<double>(leaderParts) / activeSize *
                            (1 - FLAGS_leader_balance_deviation));
      globalMax = std::floor(static_cast<double>(leaderParts) / activeSize *
                             (1 + FLAGS_leader_balance_deviation));
    }
    LOG(INFO) << "Build leader balance plan, expected min load: " << globalMin
              << ", max load: " << globalMax << " avg: " << globalAvg;

    for (auto it = allHostParts.begin(); it != allHostParts.end(); it++) {
      hostBounds_[it->first] = std::make_pair(globalMin, globalMax);
    }
  }

  while (true) {
    int32_t taskCount = 0;
    bool hasUnbalancedHost = false;
    for (const auto& hostEntry : leaderHostParts) {
      auto host = hostEntry.first;
      auto& hostMinLoad = hostBounds_[host].first;
      auto& hostMaxLoad = hostBounds_[host].second;
      int32_t partSize = hostEntry.second.size();
      if (hostMinLoad <= partSize && partSize <= hostMaxLoad) {
        LOG(INFO) << partSize << " is between min load " << hostMinLoad << " and max load "
                  << hostMaxLoad;
        continue;
      }

      hasUnbalancedHost = true;
      if (partSize < hostMinLoad) {
        // need to acquire leader from other hosts
        LOG(INFO) << "Acquire leaders to host: " << host << " loading: " << partSize
                  << " min loading " << hostMinLoad;
        taskCount += acquireLeaders(
            allHostParts, leaderHostParts, peersMap, activeHosts, host, plan, spaceId);
      } else {
        // need to transfer leader to other hosts
        LOG(INFO) << "Giveup leaders from host: " << host << " loading: " << partSize
                  << " max loading " << hostMaxLoad;
        taskCount += giveupLeaders(leaderHostParts, peersMap, activeHosts, host, plan, spaceId);
      }
    }

    // If every host is balanced or no more task during this loop, then the plan
    // is done
    if (!hasUnbalancedHost || taskCount == 0) {
      LOG(INFO) << "Not need balance";
      break;
    }
  }
  return true;
}

int32_t LeaderBalanceJobExecutor::acquireLeaders(HostParts& allHostParts,
                                                 HostParts& leaderHostParts,
                                                 PartAllocation& peersMap,
                                                 std::unordered_set<HostAddr>& activeHosts,
                                                 const HostAddr& target,
                                                 LeaderBalancePlan& plan,
                                                 GraphSpaceID spaceId) {
  // host will loop for the partition which is not leader, and try to acuire the
  // leader
  int32_t taskCount = 0;
  std::vector<PartitionID> diff;
  std::set_difference(allHostParts[target].begin(),
                      allHostParts[target].end(),
                      leaderHostParts[target].begin(),
                      leaderHostParts[target].end(),
                      std::back_inserter(diff));
  auto& targetLeaders = leaderHostParts[target];
  size_t minLoad = hostBounds_[target].first;
  for (const auto& partId : diff) {
    LOG(INFO) << "Try acquire leader for part " << partId;
    // find the leader of partId
    auto sources = peersMap[partId];
    for (const auto& source : sources) {
      if (source == target || !activeHosts.count(source)) {
        continue;
      }

      // if peer is the leader of partId and can transfer, then transfer it to
      // host
      auto& sourceLeaders = leaderHostParts[source];
      LOG(INFO) << "Check peer: " << source << " min load: " << minLoad
                << " peerLeaders size: " << sourceLeaders.size();
      auto it = std::find(sourceLeaders.begin(), sourceLeaders.end(), partId);
      if (it != sourceLeaders.end() && minLoad < sourceLeaders.size()) {
        sourceLeaders.erase(it);
        targetLeaders.emplace_back(partId);
        plan.emplace_back(spaceId, partId, source, target);
        LOG(INFO) << "acquire plan trans leader space: " << spaceId << " part: " << partId
                  << " from " << source.host << ":" << source.port << " to " << target.host << ":"
                  << target.port;
        ++taskCount;
        break;
      }
    }

    // if host has enough leader, just return
    if (targetLeaders.size() == minLoad) {
      LOG(INFO) << "Host: " << target << "'s leader reach " << minLoad;
      break;
    }
  }
  return taskCount;
}

int32_t LeaderBalanceJobExecutor::giveupLeaders(HostParts& leaderParts,
                                                PartAllocation& peersMap,
                                                std::unordered_set<HostAddr>& activeHosts,
                                                const HostAddr& source,
                                                LeaderBalancePlan& plan,
                                                GraphSpaceID spaceId) {
  int32_t taskCount = 0;
  auto& sourceLeaders = leaderParts[source];
  size_t maxLoad = hostBounds_[source].second;

  // host will try to transfer the extra leaders to other peers
  for (auto it = sourceLeaders.begin(); it != sourceLeaders.end();) {
    // find the leader of partId
    auto partId = *it;
    const auto& targets = peersMap[partId];
    bool isErase = false;

    // leader should move to the peer with lowest loading
    auto target =
        std::min_element(targets.begin(), targets.end(), [&](const auto& l, const auto& r) -> bool {
          if (source == l || !activeHosts.count(l)) {
            return false;
          }
          return leaderParts[l].size() < leaderParts[r].size();
        });

    // If peer can accept this partition leader, than host will transfer to the
    // peer
    if (target != targets.end()) {
      auto& targetLeaders = leaderParts[*target];
      int32_t targetLeaderSize = targetLeaders.size();
      if (targetLeaderSize < hostBounds_[*target].second) {
        it = sourceLeaders.erase(it);
        targetLeaders.emplace_back(partId);
        plan.emplace_back(spaceId, partId, source, *target);
        LOG(INFO) << "giveup plan trans leader space: " << spaceId << " part: " << partId
                  << " from " << source.host << ":" << source.port << " to " << target->host << ":"
                  << target->port;
        ++taskCount;
        isErase = true;
      }
    }

    // if host has enough leader, just return
    if (sourceLeaders.size() == maxLoad) {
      LOG(INFO) << "Host: " << source << "'s leader reach " << maxLoad;
      break;
    }

    if (!isErase) {
      ++it;
    }
  }
  return taskCount;
}

void LeaderBalanceJobExecutor::simplifyLeaderBalancePlan(GraphSpaceID spaceId,
                                                         LeaderBalancePlan& plan) {
  std::unordered_map<PartitionID, LeaderBalancePlan> buckets;
  for (auto& task : plan) {
    buckets[std::get<1>(task)].emplace_back(task);
  }
  plan.clear();
  for (const auto& partEntry : buckets) {
    plan.emplace_back(spaceId,
                      partEntry.first,
                      std::get<2>(partEntry.second.front()),
                      std::get<3>(partEntry.second.back()));
  }
}

}  // namespace meta
}  // namespace nebula
