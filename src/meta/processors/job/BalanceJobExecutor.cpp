/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/BalanceJobExecutor.h"

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
std::atomic_bool BalanceJobExecutor::running_ = false;
std::atomic_bool LeaderBalanceJobExecutor::inLeaderBalance_ = false;
std::unique_ptr<BalancePlan> DataBalanceJobExecutor::plan_ = nullptr;
std::mutex BalanceJobExecutor::lock_;
BalanceJobExecutor::BalanceJobExecutor(JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       const std::vector<std::string>& paras)
    : MetaJobExecutor(jobId, kvstore, adminClient, paras) {
  executor_.reset(new folly::CPUThreadPoolExecutor(1));
  toHost_ = TargetHosts::NONE;
}

bool BalanceJobExecutor::check() { return !paras_.empty(); }

nebula::cpp2::ErrorCode BalanceJobExecutor::prepare() { return nebula::cpp2::ErrorCode::SUCCEEDED; }

nebula::cpp2::ErrorCode BalanceJobExecutor::stop() {
  stopped_ = true;
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status> BalanceJobExecutor::executeInternal(HostAddr&& address,
                                                          std::vector<PartitionID>&& parts) {
  UNUSED(address);
  UNUSED(parts);
  return Status::OK();
}

bool BalanceJobExecutor::runInMeta() { return true; }

nebula::cpp2::ErrorCode BalanceJobExecutor::recovery() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::recovery() {
  if (kvstore_ == nullptr) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  auto* store = static_cast<kvstore::NebulaStore*>(kvstore_);
  if (!store->isLeader(kDefaultSpaceId, kDefaultPartId)) {
    // We need to check whether is leader or not, otherwise we would failed to
    // persist state of BalancePlan and BalanceTask, so we just reject request
    // if not leader.
    return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
  }
  auto jobKey = JobDescription::makeJobKey(jobId_);
  std::string value;
  auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, jobKey, &value);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Can't access kvstore, ret = " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto optJobRet = JobDescription::makeJobDescription(jobKey, value);
  auto optJob = nebula::value(optJobRet);
  plan_.reset(new BalancePlan(optJob, kvstore_, adminClient_));
  plan_->onFinished_ = [this]() {
    std::lock_guard<std::mutex> lg(lock_);
    if (LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec()) !=
        nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Balance plan " << plan_->id() << " update meta failed";
    }
    finishInternal();
  };
  auto recRet = plan_->recovery();
  if (recRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    plan_.reset(nullptr);
    return recRet;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::finish(bool ret) {
  UNUSED(ret);
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::getAllSpaces(
    std::vector<std::tuple<GraphSpaceID, int32_t, bool>>& spaces) {
  // Get all spaces
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::spacePrefix();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Get all spaces failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  while (iter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(iter->key());
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    bool zoned = properties.group_name_ref().has_value();
    spaces.emplace_back(spaceId, *properties.replica_factor_ref(), zoned);
    iter->next();
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::save(const std::string& k, const std::string& v) {
  std::vector<kvstore::KV> data{std::make_pair(k, v)};
  folly::Baton<true, std::atomic> baton;
  auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        rc = code;
        baton.post();
      });
  baton.wait();
  return rc;
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::buildBalancePlan() {
  if (plan_ != nullptr) {
    LOG(ERROR) << "Balance plan should be nullptr now";
    return nebula::cpp2::ErrorCode::E_BALANCED;
  }
  std::vector<std::tuple<GraphSpaceID, int32_t, bool>> spaces;
  auto spacesRet = getAllSpaces(spaces);
  if (spacesRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Can't get all spaces";
    return spacesRet;
  }

  plan_.reset(new BalancePlan(jobDescription_, kvstore_, adminClient_));
  for (const auto& spaceInfo : spaces) {
    auto spaceId = std::get<0>(spaceInfo);
    auto spaceReplica = std::get<1>(spaceInfo);
    auto dependentOnGroup = std::get<2>(spaceInfo);
    LOG(INFO) << "Balance Space " << spaceId;
    auto taskRet = genTasks(spaceId, spaceReplica, dependentOnGroup, lostHosts_);
    if (!ok(taskRet)) {
      LOG(ERROR) << "Generate tasks on space " << std::get<0>(spaceInfo) << " failed";
      return error(taskRet);
    }

    auto tasks = std::move(value(taskRet));
    for (auto& task : tasks) {
      plan_->addTask(std::move(task));
    }
  }

  plan_->onFinished_ = [this]() {
    std::lock_guard<std::mutex> lg(lock_);
    if (LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec()) !=
        nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Balance plan " << plan_->id() << " update meta failed";
    }
    finishInternal();
  };
  if (plan_->tasks_.empty()) {
    return nebula::cpp2::ErrorCode::E_BALANCED;
  }
  return plan_->saveInStore();
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>> DataBalanceJobExecutor::genTasks(
    GraphSpaceID spaceId,
    int32_t spaceReplica,
    bool dependentOnGroup,
    std::vector<HostAddr>& lostHosts) {
  HostParts hostParts;
  int32_t totalParts = 0;
  // hostParts is current part allocation map
  auto result = getHostParts(spaceId, dependentOnGroup, hostParts, totalParts);
  if (!nebula::ok(result)) {
    return nebula::error(result);
  }

  auto retVal = nebula::value(result);
  if (!retVal || totalParts == 0 || hostParts.empty()) {
    LOG(ERROR) << "Invalid space " << spaceId;
    return nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
  }

  auto fetchHostPartsRet = fetchHostParts(spaceId, dependentOnGroup, hostParts, lostHosts);
  if (!nebula::ok(fetchHostPartsRet)) {
    LOG(ERROR) << "Fetch hosts and parts failed";
    return nebula::error(fetchHostPartsRet);
  }

  auto hostPartsRet = nebula::value(fetchHostPartsRet);
  auto confirmedHostParts = hostPartsRet.first;
  auto activeHosts = hostPartsRet.second;
  LOG(INFO) << "Now, try to balance the confirmedHostParts";

  // We have two parts need to balance, the first one is parts on lost hosts and
  // deleted hosts The seconds one is parts on unbalanced host in
  // confirmedHostParts.
  std::vector<BalanceTask> tasks;
  // 1. Iterate through all hosts that would not be included in
  // confirmedHostParts,
  //    move all parts in them to host with minimum part in confirmedHostParts
  for (auto& lostHost : lostHosts) {
    auto& lostParts = hostParts[lostHost];
    for (auto& partId : lostParts) {
      LOG(INFO) << "Try balance part " << partId << " for lost host " << lostHost;
      // check whether any peers which is alive
      auto alive = checkReplica(hostParts, activeHosts, spaceReplica, partId);
      if (!alive.ok()) {
        LOG(ERROR) << "Check Replica failed: " << alive << " Part: " << partId;
        return nebula::cpp2::ErrorCode::E_NO_VALID_HOST;
      }

      auto retCode =
          transferLostHost(tasks, confirmedHostParts, lostHost, spaceId, partId, dependentOnGroup);
      if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Transfer lost host " << lostHost << " failed";
        return retCode;
      }
    }
  }

  // 2. Make all hosts in confirmedHostParts balanced
  if (balanceParts(spaceId, confirmedHostParts, totalParts, tasks, dependentOnGroup)) {
    return tasks;
  } else {
    return nebula::cpp2::ErrorCode::E_BAD_BALANCE_PLAN;
  }
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::transferLostHost(std::vector<BalanceTask>& tasks,
                                                                 HostParts& confirmedHostParts,
                                                                 const HostAddr& source,
                                                                 GraphSpaceID spaceId,
                                                                 PartitionID partId,
                                                                 bool dependentOnGroup) {
  // find a host with minimum parts which doesn't have this part
  ErrorOr<nebula::cpp2::ErrorCode, HostAddr> result;
  if (dependentOnGroup) {
    result = hostWithMinimalPartsForZone(source, confirmedHostParts, partId);
  } else {
    result = hostWithMinimalParts(confirmedHostParts, partId);
  }

  if (!nebula::ok(result)) {
    LOG(ERROR) << "Can't find a host which doesn't have part: " << partId;
    return nebula::error(result);
  }
  const auto& targetHost = nebula::value(result);
  confirmedHostParts[targetHost].emplace_back(partId);
  tasks.emplace_back(plan_->id(), spaceId, partId, source, targetHost, kvstore_, adminClient_);
  zoneParts_[targetHost].second.emplace_back(partId);
  auto zoneIt =
      std::find(zoneParts_[source].second.begin(), zoneParts_[source].second.end(), partId);
  if (zoneIt == zoneParts_[source].second.end()) {
    LOG(ERROR) << "part not find " << partId << " at " << source;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::pair<HostParts, std::vector<HostAddr>>>
DataBalanceJobExecutor::fetchHostParts(GraphSpaceID spaceId,
                                       bool dependentOnGroup,
                                       const HostParts& hostParts,
                                       std::vector<HostAddr>& lostHosts) {
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> activeHostsRet;
  if (dependentOnGroup) {
    activeHostsRet = ActiveHostsMan::getActiveHostsWithGroup(kvstore_, spaceId);
  } else {
    activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  }

  if (!nebula::ok(activeHostsRet)) {
    return nebula::error(activeHostsRet);
  }

  std::vector<HostAddr> expand;
  auto activeHosts = nebula::value(activeHostsRet);
  calDiff(hostParts, activeHosts, expand, lostHosts);
  // confirmedHostParts is new part allocation map after balance, it would
  // include newlyAdded and exclude lostHosts
  HostParts confirmedHostParts(hostParts);
  for (const auto& h : expand) {
    LOG(INFO) << "Found new host " << h;
    confirmedHostParts.emplace(h, std::vector<PartitionID>());
  }
  for (const auto& h : lostHosts) {
    LOG(INFO) << "Lost host " << h;
    confirmedHostParts.erase(h);
  }
  return std::make_pair(confirmedHostParts, activeHosts);
}

bool DataBalanceJobExecutor::balanceParts(GraphSpaceID spaceId,
                                          HostParts& confirmedHostParts,
                                          int32_t totalParts,
                                          std::vector<BalanceTask>& tasks,
                                          bool dependentOnGroup) {
  auto avgLoad = static_cast<float>(totalParts) / confirmedHostParts.size();
  VLOG(3) << "The expect avg load is " << avgLoad;
  int32_t minLoad = std::floor(avgLoad);
  int32_t maxLoad = std::ceil(avgLoad);
  VLOG(3) << "The min load is " << minLoad << " max load is " << maxLoad;

  auto sortedHosts = sortedHostsByParts(confirmedHostParts);
  if (sortedHosts.empty()) {
    LOG(ERROR) << "Host is empty";
    return false;
  }

  auto maxPartsHost = sortedHosts.back();
  auto minPartsHost = sortedHosts.front();
  auto& sourceHost = maxPartsHost.first;
  auto& targetHost = minPartsHost.first;
  if (innerBalance_) {
    LOG(INFO) << "maxPartsHost.first " << maxPartsHost.first << " minPartsHost.first "
              << minPartsHost.first;
    while (!checkZoneLegal(maxPartsHost.first, minPartsHost.first)) {
      sortedHosts.pop_back();
      maxPartsHost = sortedHosts.back();
    }

    auto& source = maxPartsHost.first;
    auto iter = std::find_if(zoneParts_.begin(), zoneParts_.end(), [&source](const auto& pair) {
      return source == pair.first;
    });

    auto& zoneName = iter->second.first;
    int32_t hostsSize = zoneHosts_[zoneName].size();
    int32_t totalPartsZone = 0;
    for (auto& host : zoneHosts_[zoneName]) {
      auto it = confirmedHostParts.find(host);
      if (it == confirmedHostParts.end()) {
        LOG(ERROR) << "Host " << host << "not in confirmedHostParts";
        continue;
      }
      totalPartsZone += it->second.size();
    }

    avgLoad = static_cast<float>(totalPartsZone) / hostsSize;
    minLoad = std::floor(avgLoad);
    maxLoad = std::ceil(avgLoad);
    LOG(INFO) << "Update min and max loading Total parts in zone " << totalPartsZone
              << ", total hosts " << hostsSize << " The expect avg load is " << avgLoad
              << " The min load is " << minLoad << " max load is " << maxLoad;
  }

  while (maxPartsHost.second > maxLoad || minPartsHost.second < minLoad) {
    auto& partsFrom = confirmedHostParts[maxPartsHost.first];
    auto& partsTo = confirmedHostParts[minPartsHost.first];
    std::sort(partsFrom.begin(), partsFrom.end());
    std::sort(partsTo.begin(), partsTo.end());

    LOG(INFO) << maxPartsHost.first << ":" << partsFrom.size() << " -> " << minPartsHost.first
              << ":" << partsTo.size();
    std::vector<PartitionID> diff;
    std::set_difference(partsFrom.begin(),
                        partsFrom.end(),
                        partsTo.begin(),
                        partsTo.end(),
                        std::inserter(diff, diff.begin()));
    bool noAction = true;
    for (auto& partId : diff) {
      LOG(INFO) << "partsFrom size " << partsFrom.size() << " partsTo size " << partsTo.size()
                << " minLoad " << minLoad << " maxLoad " << maxLoad;
      if (partsFrom.size() == partsTo.size() + 1 ||
          partsFrom.size() == static_cast<size_t>(minLoad) ||
          partsTo.size() == static_cast<size_t>(maxLoad)) {
        VLOG(3) << "No need to move any parts from " << maxPartsHost.first << " to "
                << minPartsHost.first;
        break;
      }

      LOG(INFO) << "[space:" << spaceId << ", part:" << partId << "] " << maxPartsHost.first << "->"
                << minPartsHost.first;
      auto it = std::find(partsFrom.begin(), partsFrom.end(), partId);
      if (it == partsFrom.end()) {
        LOG(ERROR) << "Part " << partId << " not found in partsFrom";
        return false;
      }

      if (std::find(partsTo.begin(), partsTo.end(), partId) != partsTo.end()) {
        LOG(ERROR) << "Part " << partId << " already existed in partsTo";
        return false;
      }

      if (dependentOnGroup) {
        if (!checkZoneLegal(sourceHost, targetHost)) {
          LOG(INFO) << "sourceHost " << sourceHost << " targetHost " << targetHost
                    << " not same zone";

          auto& parts = relatedParts_[targetHost];
          auto minIt = std::find(parts.begin(), parts.end(), partId);
          if (minIt != parts.end()) {
            LOG(INFO) << "Part " << partId << " have existed";
            continue;
          }
        }

        auto& sourceNoneName = zoneParts_[sourceHost].first;
        auto sourceHosts = zoneHosts_.find(sourceNoneName);
        for (auto& sh : sourceHosts->second) {
          auto& parts = relatedParts_[sh];
          auto maxIt = std::find(parts.begin(), parts.end(), partId);
          if (maxIt == parts.end()) {
            LOG(INFO) << "Part " << partId << " not found on " << sh;
            continue;
          }
          parts.erase(maxIt);
        }

        auto& targetNoneName = zoneParts_[targetHost].first;
        auto targetHosts = zoneHosts_.find(targetNoneName);
        for (auto& th : targetHosts->second) {
          relatedParts_[th].emplace_back(partId);
        }
      }

      partsFrom.erase(it);
      partsTo.emplace_back(partId);
      tasks.emplace_back(
          jobId_, spaceId, partId, maxPartsHost.first, minPartsHost.first, kvstore_, adminClient_);
      noAction = false;
    }

    if (noAction) {
      LOG(INFO) << "Here is no action";
      break;
    }
    sortedHosts = sortedHostsByParts(confirmedHostParts);
    maxPartsHost = sortedHosts.back();
    minPartsHost = sortedHosts.front();
    if (innerBalance_) {
      while (!checkZoneLegal(maxPartsHost.first, minPartsHost.first)) {
        sortedHosts.pop_back();
        maxPartsHost = sortedHosts.back();
      }

      auto& source = maxPartsHost.first;
      auto iter = std::find_if(zoneParts_.begin(), zoneParts_.end(), [&source](const auto& pair) {
        return source == pair.first;
      });

      auto& zoneName = iter->second.first;
      int32_t hostsSize = zoneHosts_[zoneName].size();
      int32_t totalPartsZone = 0;
      for (auto& host : zoneHosts_[zoneName]) {
        auto it = confirmedHostParts.find(host);
        if (it == confirmedHostParts.end()) {
          LOG(ERROR) << "Host " << host << "not in confirmedHostParts";
          continue;
        }
        totalPartsZone += it->second.size();
      }

      avgLoad = static_cast<float>(totalPartsZone) / hostsSize;
      minLoad = std::floor(avgLoad);
      maxLoad = std::ceil(avgLoad);
      LOG(INFO) << "Update min and max loading Total parts in zone " << totalPartsZone
                << ", total hosts " << hostsSize << " The expect avg load is " << avgLoad
                << " The min load is " << minLoad << " max load is " << maxLoad;
    }
  }
  LOG(INFO) << "Balance tasks num: " << tasks.size();
  for (auto& task : tasks) {
    LOG(INFO) << task.taskIdStr();
  }

  relatedParts_.clear();
  return true;
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::stop() {
  std::lock_guard<std::mutex> lg(lock_);
  if (!running_) {
    return nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
  }
  stopped_ = true;
  plan_->stop();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, bool> BalanceJobExecutor::getHostParts(GraphSpaceID spaceId,
                                                                        bool dependentOnGroup,
                                                                        HostParts& hostParts,
                                                                        int32_t& totalParts) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId << " "
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
    LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId
               << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto properties = MetaKeyUtils::parseSpace(value);
  if (totalParts != properties.get_partition_num()) {
    LOG(ERROR) << "Partition number not equals";
    LOG(ERROR) << totalParts << " : " << properties.get_partition_num();
    return false;
  }

  int32_t replica = properties.get_replica_factor();
  LOG(INFO) << "Replica " << replica;
  if (dependentOnGroup && properties.group_name_ref().has_value()) {
    auto groupName = *properties.group_name_ref();
    auto groupKey = MetaKeyUtils::groupKey(groupName);
    std::string groupValue;
    retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Get group " << groupName
                 << " failed: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    int32_t zoneSize = MetaKeyUtils::parseZoneNames(std::move(groupValue)).size();
    LOG(INFO) << "Zone Size " << zoneSize;
    innerBalance_ = (replica == zoneSize);

    auto activeHostsRet = ActiveHostsMan::getActiveHostsWithGroup(kvstore_, spaceId);
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

    auto zonePartsRet = assembleZoneParts(groupName, confirmedHostParts);
    if (zonePartsRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Assemble Zone Parts failed group: " << groupName;
      return zonePartsRet;
    }
  }

  totalParts *= replica;
  return true;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::assembleZoneParts(const std::string& groupName,
                                                              HostParts& hostParts) {
  LOG(INFO) << "Balancer assembleZoneParts";
  auto groupKey = MetaKeyUtils::groupKey(groupName);
  std::string groupValue;
  auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Get group " << groupName
               << " failed: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  // zoneHosts use to record this host belong to zone's hosts
  std::unordered_map<std::pair<HostAddr, std::string>, std::vector<HostAddr>> zoneHosts;
  auto zoneNames = MetaKeyUtils::parseZoneNames(std::move(groupValue));
  for (const auto& zoneName : zoneNames) {
    LOG(INFO) << "Zone Name: " << zoneName;
    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    std::string zoneValue;
    retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Get zone " << zoneName
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

void BalanceJobExecutor::calDiff(const HostParts& hostParts,
                                 const std::vector<HostAddr>& activeHosts,
                                 std::vector<HostAddr>& expand,
                                 std::vector<HostAddr>& lost) {
  for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
    VLOG(1) << "Original Host " << it->first << ", parts " << it->second.size();
    if (std::find(activeHosts.begin(), activeHosts.end(), it->first) == activeHosts.end() &&
        std::find(lost.begin(), lost.end(), it->first) == lost.end()) {
      lost.emplace_back(it->first);
    }
  }
  for (auto& h : activeHosts) {
    VLOG(1) << "Active host " << h;
    if (hostParts.find(h) == hostParts.end()) {
      expand.emplace_back(h);
    }
  }
}

std::vector<std::pair<HostAddr, int32_t>> DataBalanceJobExecutor::sortedHostsByParts(
    const HostParts& hostParts) {
  std::vector<std::pair<HostAddr, int32_t>> hosts;
  for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
    LOG(INFO) << "Host " << it->first << " parts " << it->second.size();
    hosts.emplace_back(it->first, it->second.size());
  }
  std::sort(hosts.begin(), hosts.end(), [](const auto& l, const auto& r) {
    if (l.second != r.second) {
      return l.second < r.second;
    } else {
      return l.first.host < r.first.host;
    }
  });
  return hosts;
}

Status DataBalanceJobExecutor::checkReplica(const HostParts& hostParts,
                                            const std::vector<HostAddr>& activeHosts,
                                            int32_t replica,
                                            PartitionID partId) {
  // check host hold the part and alive
  auto checkPart = [&](const auto& entry) {
    auto& host = entry.first;
    auto& parts = entry.second;
    return std::find(parts.begin(), parts.end(), partId) != parts.end() &&
           std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end();
  };
  auto aliveReplica = std::count_if(hostParts.begin(), hostParts.end(), checkPart);
  if (aliveReplica >= replica / 2 + 1) {
    return Status::OK();
  }
  return Status::Error("Not enough alive host hold the part %d", partId);
}

ErrorOr<nebula::cpp2::ErrorCode, HostAddr> DataBalanceJobExecutor::hostWithMinimalParts(
    const HostParts& hostParts, PartitionID partId) {
  auto hosts = sortedHostsByParts(hostParts);
  for (auto& h : hosts) {
    auto it = hostParts.find(h.first);
    if (it == hostParts.end()) {
      LOG(ERROR) << "Host " << h.first << " not found";
      return nebula::cpp2::ErrorCode::E_NO_HOSTS;
    }

    if (std::find(it->second.begin(), it->second.end(), partId) == it->second.end()) {
      return h.first;
    }
  }
  return nebula::cpp2::ErrorCode::E_NO_HOSTS;
}

ErrorOr<nebula::cpp2::ErrorCode, HostAddr> DataBalanceJobExecutor::hostWithMinimalPartsForZone(
    const HostAddr& source, const HostParts& hostParts, PartitionID partId) {
  auto hosts = sortedHostsByParts(hostParts);
  for (auto& h : hosts) {
    auto it = hostParts.find(h.first);
    if (it == hostParts.end()) {
      LOG(ERROR) << "Host " << h.first << " not found";
      return nebula::cpp2::ErrorCode::E_NO_HOSTS;
    }

    LOG(INFO) << "source " << source << " h.first " << h.first;
    if (std::find(it->second.begin(), it->second.end(), partId) == it->second.end()) {
      return h.first;
    }
  }
  return nebula::cpp2::ErrorCode::E_NO_HOSTS;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::collectZoneParts(const std::string& groupName,
                                                             HostParts& hostParts) {
  auto groupKey = MetaKeyUtils::groupKey(groupName);
  std::string groupValue;
  auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Get group " << groupName
               << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  // zoneHosts use to record this host belong to zone's hosts
  std::unordered_map<std::pair<HostAddr, std::string>, std::vector<HostAddr>> zoneHosts;
  auto zoneNames = MetaKeyUtils::parseZoneNames(std::move(groupValue));
  for (auto zoneName : zoneNames) {
    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    std::string zoneValue;
    retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Get zone " << zoneName
                 << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    auto hosts = MetaKeyUtils::parseZoneHosts(std::move(zoneValue));
    for (const auto& host : hosts) {
      auto pair = std::pair<HostAddr, std::string>(std::move(host), zoneName);
      auto& hs = zoneHosts[std::move(pair)];
      hs.insert(hs.end(), hosts.begin(), hosts.end());
    }
  }

  for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
    auto host = it->first;
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
    for (auto hostIter = hosts.begin(); hostIter != hosts.end(); hostIter++) {
      auto partIter = hostParts.find(*hostIter);
      if (partIter == hostParts.end()) {
        zoneParts_[it->first] = ZoneNameAndParts(name, std::vector<PartitionID>());
      } else {
        zoneParts_[it->first] = ZoneNameAndParts(name, partIter->second);
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

bool DataBalanceJobExecutor::checkZoneLegal(const HostAddr& source, const HostAddr& target) {
  VLOG(3) << "Check " << source << " : " << target;
  auto sourceIter = std::find_if(zoneParts_.begin(), zoneParts_.end(), [&source](const auto& pair) {
    return source == pair.first;
  });

  if (sourceIter == zoneParts_.end()) {
    LOG(INFO) << "Source " << source << " not found";
    return false;
  }

  auto targetIter = std::find_if(zoneParts_.begin(), zoneParts_.end(), [&target](const auto& pair) {
    return target == pair.first;
  });

  if (targetIter == zoneParts_.end()) {
    LOG(INFO) << "Target " << target << " not found";
    return false;
  }

  LOG(INFO) << sourceIter->second.first << " : " << targetIter->second.first;
  return sourceIter->second.first == targetIter->second.first;
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::prepare() {
  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  if (!nebula::ok(activeHostsRet)) {
    auto retCode = nebula::error(activeHostsRet);
    LOG(ERROR) << "Get active hosts failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto hosts = std::move(nebula::value(activeHostsRet));

  if (hosts.empty()) {
    LOG(ERROR) << "There is no active hosts";
    return nebula::cpp2::ErrorCode::E_NO_HOSTS;
  }
  lostHosts_.reserve(paras_.size() - 1);
  for (size_t i = 0; i < paras_.size() - 1; i++) {
    lostHosts_.emplace_back(HostAddr::fromString(paras_[i]));
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::finish(bool ret) {
  std::lock_guard<std::mutex> lg(lock_);
  return finishInternal(ret);
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::finishInternal(bool ret) {
  CHECK(!lock_.try_lock());
  plan_.reset(nullptr);
  running_ = false;
  auto rc = onFinished_(ret);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }
  return ret ? nebula::cpp2::ErrorCode::SUCCEEDED : nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
}

folly::Future<Status> DataBalanceJobExecutor::executeInternal(HostAddr&& address,
                                                              std::vector<PartitionID>&& parts) {
  UNUSED(address);
  UNUSED(parts);
  std::unique_lock<std::mutex> lg(lock_);
  if (!running_) {
    if (plan_ == nullptr) {
      auto retCode = buildBalancePlan();
      if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        if (retCode == nebula::cpp2::ErrorCode::E_BALANCED) {
          finishInternal(true);
          return Status::OK();
        } else {
          return Status::Error(apache::thrift::util::enumNameSafe(retCode));
        }
      }
    }
    LOG(INFO) << "Start to invoke balance plan " << plan_->id();
    running_ = true;
    auto fut = folly::via(executor_.get(), std::bind(&BalancePlan::invoke, plan_.get()));
    lg.unlock();
    fut.wait();
    return Status::OK();
  }
  CHECK(plan_ != nullptr);
  LOG(INFO) << "Balance job " << plan_->id() << " is still running";
  return Status::Error(folly::sformat("Balance job {} is still running", plan_->id()));
}

folly::Future<Status> LeaderBalanceJobExecutor::executeInternal(HostAddr&& address,
                                                                std::vector<PartitionID>&& parts) {
  UNUSED(address);
  UNUSED(parts);
  if (running_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Balance process still running";
    return Status::OK();
  }

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
      auto dependentOnGroup = std::get<2>(spaceInfo);
      LeaderBalancePlan plan;
      auto balanceResult = buildLeaderBalancePlan(
          hostLeaderMap_.get(), spaceId, replicaFactor, dependentOnGroup, plan);
      if (!nebula::ok(balanceResult) || !nebula::value(balanceResult)) {
        LOG(ERROR) << "Building leader balance plan failed "
                   << "Space: " << spaceId;
        continue;
      }
      simplifyLeaderBalnacePlan(spaceId, plan);
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
      LOG(ERROR) << failed << " partiton failed to transfer leader";
    }
    onFinished_(false);
    return Status::Error("partiton failed to transfer leader");
  }
  onFinished_(true);
  return Status::OK();
}

ErrorOr<nebula::cpp2::ErrorCode, bool> LeaderBalanceJobExecutor::buildLeaderBalancePlan(
    HostLeaderMap* hostLeaderMap,
    GraphSpaceID spaceId,
    int32_t replicaFactor,
    bool dependentOnGroup,
    LeaderBalancePlan& plan,
    bool useDeviation) {
  PartAllocation peersMap;
  HostParts leaderHostParts;
  size_t leaderParts = 0;
  // store peers of all paritions in peerMap
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId << static_cast<int32_t>(retCode);
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
  auto result = getHostParts(spaceId, dependentOnGroup, allHostParts, totalParts);
  if (!nebula::ok(result)) {
    return nebula::error(result);
  } else {
    auto retVal = nebula::value(result);
    if (!retVal || totalParts == 0 || allHostParts.empty()) {
      LOG(ERROR) << "Invalid space " << spaceId;
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
    LOG(ERROR) << "No active hosts";
    return false;
  }

  if (dependentOnGroup) {
    for (auto it = allHostParts.begin(); it != allHostParts.end(); it++) {
      auto min = it->second.size() / replicaFactor;
      VLOG(3) << "Host: " << it->first << " Bounds: " << min << " : " << min + 1;
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
    VLOG(3) << "Build leader balance plan, expected min load: " << globalMin
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
        VLOG(3) << partSize << " is between min load " << hostMinLoad << " and max load "
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
    VLOG(3) << "Try acquire leader for part " << partId;
    // find the leader of partId
    auto sources = peersMap[partId];
    for (const auto& source : sources) {
      if (source == target || !activeHosts.count(source)) {
        continue;
      }

      // if peer is the leader of partId and can transfer, then transfer it to
      // host
      auto& sourceLeaders = leaderHostParts[source];
      VLOG(3) << "Check peer: " << source << " min load: " << minLoad
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

void LeaderBalanceJobExecutor::simplifyLeaderBalnacePlan(GraphSpaceID spaceId,
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
