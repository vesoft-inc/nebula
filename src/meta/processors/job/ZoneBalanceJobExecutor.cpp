/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/ZoneBalanceJobExecutor.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include <memory>

#include "common/utils/MetaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobUtils.h"

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode ZoneBalanceJobExecutor::prepare() {
  auto spaceRet = getSpaceIdFromName(paras_.back());
  if (!nebula::ok(spaceRet)) {
    LOG(INFO) << "Can't find the space: " << paras_.back();
    return nebula::error(spaceRet);
  }
  GraphSpaceID spaceId = nebula::value(spaceRet);
  nebula::cpp2::ErrorCode rc = spaceInfo_.loadInfo(spaceId, kvstore_);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }
  lostZones_.reserve(paras_.size() - 1);
  for (size_t i = 0; i < paras_.size() - 1; i++) {
    lostZones_.emplace_back(paras_[i]);
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode ZoneBalanceJobExecutor::stop() {
  plan_->stop();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status> ZoneBalanceJobExecutor::executeInternal() {
  if (plan_ == nullptr) {
    Status status = buildBalancePlan();
    if (status != Status::OK()) {
      if (status == Status::Balanced()) {
        executorOnFinished_(meta::cpp2::JobStatus::FINISHED);
        return Status::OK();
      }
      return status;
    }
  }
  plan_->setFinishCallBack([this](meta::cpp2::JobStatus status) {
    if (status == meta::cpp2::JobStatus::FINISHED) {
      folly::SharedMutex::WriteHolder holder(LockUtils::lock());
      nebula::cpp2::ErrorCode ret = updateMeta();
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        status = meta::cpp2::JobStatus::FAILED;
      }
    }
    executorOnFinished_(status);
  });
  plan_->invoke();
  return Status::OK();
}

nebula::cpp2::ErrorCode ZoneBalanceJobExecutor::updateMeta() {
  std::string spaceKey = MetaKeyUtils::spaceKey(spaceInfo_.spaceId_);
  std::string spaceVal;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceVal);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }

  meta::cpp2::SpaceDesc properties = MetaKeyUtils::parseSpace(spaceVal);
  std::vector<std::string> zones;
  for (std::string& zn : lostZones_) {
    spaceInfo_.zones_.erase(zn);
  }
  for (auto& zoneMapEntry : spaceInfo_.zones_) {
    zones.emplace_back(zoneMapEntry.first);
  }
  properties.zone_names_ref() = std::move(zones);
  std::vector<kvstore::KV> data;
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  data.emplace_back(MetaKeyUtils::spaceKey(spaceInfo_.spaceId_),
                    MetaKeyUtils::spaceVal(properties));
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncMultiPut(kDefaultSpaceId,
                          kDefaultPartId,
                          std::move(data),
                          [&baton, &ret](nebula::cpp2::ErrorCode code) {
                            if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                              ret = code;
                              LOG(INFO) << "Can't write the kvstore, ret = "
                                        << static_cast<int32_t>(code);
                            }
                            baton.post();
                          });
  baton.wait();
  return ret;
}

HostAddr ZoneBalanceJobExecutor::insertPartIntoZone(
    std::map<std::string, std::vector<Host*>>* sortedZoneHosts, Zone* zone, PartitionID partId) {
  std::vector<Host*>& sortedHosts = sortedZoneHosts->operator[](zone->zoneName_);
  sortedHosts.front()->parts_.emplace(partId);
  zone->partNum_++;
  HostAddr ha = sortedHosts.front()->host_;
  for (size_t i = 0; i < sortedHosts.size() - 1; i++) {
    if (sortedHosts[i]->parts_.size() >= sortedHosts[i + 1]->parts_.size()) {
      std::swap(sortedHosts[i], sortedHosts[i + 1]);
    } else {
      break;
    }
  }
  return ha;
}

nebula::cpp2::ErrorCode ZoneBalanceJobExecutor::rebalanceActiveZones(
    std::vector<Zone*>* sortedActiveZones,
    std::map<std::string, std::vector<Host*>>* sortedZoneHosts,
    std::map<PartitionID, std::vector<BalanceTask>>* existTasks) {
  std::vector<Zone*>& sortedActiveZonesRef = *sortedActiveZones;
  std::map<std::string, std::vector<Host*>>& sortedZoneHostsRef = *sortedZoneHosts;
  int32_t totalPartNum = 0;
  int32_t avgPartNum = 0;
  for (auto& z : sortedActiveZonesRef) {
    totalPartNum += z->partNum_;
  }
  if (sortedActiveZonesRef.size() == 0) {
    LOG(INFO) << "rebalance error: no active zones";
    return nebula::cpp2::ErrorCode::E_NO_HOSTS;
  }
  avgPartNum = totalPartNum / sortedActiveZonesRef.size();
  int32_t remainder = totalPartNum - avgPartNum * sortedActiveZonesRef.size();
  int32_t leftBegin = 0;
  int32_t leftEnd = 0;
  int32_t rightBegin = 0;
  int32_t rightEnd = sortedActiveZonesRef.size();
  for (size_t i = 0; i < sortedActiveZonesRef.size(); i++) {
    if (avgPartNum <= sortedActiveZonesRef[i]->partNum_) {
      leftEnd = i;
      break;
    }
  }
  for (size_t i = leftEnd; i < sortedActiveZonesRef.size(); i++) {
    if (avgPartNum < sortedActiveZonesRef[i]->partNum_) {
      rightBegin = i;
      break;
    }
  }
  auto findZoneToInsert = [&](PartitionID partId, const HostAddr& srcHost) -> bool {
    for (int32_t leftIndex = leftBegin; leftIndex < leftEnd; leftIndex++) {
      if (!sortedActiveZonesRef[leftIndex]->partExist(partId)) {
        HostAddr dst = insertPartIntoZone(sortedZoneHosts, sortedActiveZonesRef[leftIndex], partId);
        insertOneTask(
            BalanceTask(jobId_, spaceInfo_.spaceId_, partId, srcHost, dst, kvstore_, adminClient_),
            existTasks);
        int32_t newLeftIndex = leftIndex;
        for (; newLeftIndex < leftEnd - 1; newLeftIndex++) {
          if (sortedActiveZonesRef[newLeftIndex]->partNum_ >
              sortedActiveZonesRef[newLeftIndex + 1]->partNum_) {
            std::swap(sortedActiveZonesRef[newLeftIndex], sortedActiveZonesRef[newLeftIndex + 1]);
          } else {
            break;
          }
        }
        // if the zone's part reach the avgPartNum,it can't recieve parts any more
        if (newLeftIndex == leftEnd - 1 &&
            sortedActiveZonesRef[newLeftIndex]->partNum_ >= avgPartNum) {
          leftEnd--;
        }
        // all zones in left side have reached avgPartNum,and now some of them will take
        // avgPartNum+1 if there still has remainder
        if (leftBegin == leftEnd) {
          leftEnd = rightBegin;
        }
        return true;
      }
    }
    return false;
  };
  for (int32_t right = rightBegin; right < rightEnd;) {
    Zone* srcZone = sortedActiveZonesRef[right];
    // if remainder>0 some zones will hold avgPartNum+1 patrs, we prioritise choosing zones in right
    // side to hold them
    if (srcZone->partNum_ == avgPartNum + 1 && remainder) {
      right++;
      remainder--;
      continue;
    }
    if (srcZone->partNum_ == avgPartNum) {
      right++;
      continue;
    }
    std::vector<Host*>& sortedHosts = sortedZoneHostsRef[srcZone->zoneName_];
    int32_t hostIndex = sortedHosts.size() - 1;
    // to find a part to move,we prioritise moving parts from who has the most
    for (; hostIndex >= 0; hostIndex--) {
      std::set<PartitionID>& hostParts = sortedHosts[hostIndex]->parts_;
      PartitionID movePart = -1;
      for (PartitionID partId : hostParts) {
        // to find a zone which does not contain the part in the left side to insert
        bool matched = findZoneToInsert(partId, sortedHosts[hostIndex]->host_);
        if (matched) {
          movePart = partId;
          break;
        }
      }
      if (movePart != -1) {
        hostParts.erase(movePart);
        srcZone->partNum_--;
        break;
      }
    }
    for (int32_t i = hostIndex; i > 0; i--) {
      if (sortedHosts[i]->parts_.size() <= sortedHosts[i - 1]->parts_.size()) {
        std::swap(sortedHosts[i], sortedHosts[i - 1]);
      } else {
        break;
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

/* first, move the lostZones' parts to the active zones
 * second, make balance for the active zones */
Status ZoneBalanceJobExecutor::buildBalancePlan() {
  for (std::string& zn : lostZones_) {
    if (!spaceInfo_.zones_.count(zn)) {
      return Status::Error("space %s does not have zone %s", spaceInfo_.name_.c_str(), zn.c_str());
    }
  }

  std::map<std::string, Zone*> activeZones;
  std::map<std::string, Zone*> lostZones;
  for (auto& zoneMapEntry : spaceInfo_.zones_) {
    activeZones.emplace(zoneMapEntry.first, &zoneMapEntry.second);
  }
  for (std::string& zn : lostZones_) {
    auto it = activeZones.find(zn);
    if (it != activeZones.end()) {
      lostZones.emplace(it->first, it->second);
      activeZones.erase(it);
    }
  }
  int32_t activeSize = activeZones.size();
  if (activeSize < spaceInfo_.replica_) {
    return Status::Error("Not enough alive zones to hold replica");
  }

  std::vector<Zone*> sortedActiveZones;
  sortedActiveZones.reserve(activeZones.size());
  std::map<std::string, std::vector<Host*>> sortedZoneHosts;
  std::for_each(activeZones.begin(),
                activeZones.end(),
                [&sortedActiveZones,
                 &sortedZoneHosts](std::pair<const std::string, Zone*>& activeZonesEntry) {
                  sortedActiveZones.push_back(activeZonesEntry.second);
                  std::vector<Host*>& hosts = sortedZoneHosts[activeZonesEntry.first];
                  for (auto& hostMapEntry : activeZonesEntry.second->hosts_) {
                    hosts.push_back(&hostMapEntry.second);
                  }
                  std::sort(hosts.begin(), hosts.end(), [](Host*& l, Host*& r) -> bool {
                    return l->parts_.size() < r->parts_.size();
                  });
                  sortedActiveZones.back()->calPartNum();
                });
  std::sort(sortedActiveZones.begin(), sortedActiveZones.end(), [](Zone*& l, Zone*& r) -> bool {
    return l->partNum_ < r->partNum_;
  });

  auto chooseZoneToInsert =
      [this, &sortedActiveZones, &sortedZoneHosts](PartitionID partId) -> HostAddr {
    size_t index = 0;
    for (size_t i = 0; i < sortedActiveZones.size(); i++) {
      if (!sortedActiveZones[i]->partExist(partId)) {
        index = i;
        break;
      }
    }
    HostAddr ha = insertPartIntoZone(&sortedZoneHosts, sortedActiveZones[index], partId);
    for (size_t i = index; i < sortedActiveZones.size() - 1; i++) {
      if (sortedActiveZones[i]->partNum_ >= sortedActiveZones[i + 1]->partNum_) {
        std::swap(sortedActiveZones[i], sortedActiveZones[i + 1]);
      } else {
        break;
      }
    }
    return ha;
  };

  std::map<PartitionID, std::vector<BalanceTask>> existTasks;
  // move parts of lost zones to active zones
  for (auto& zoneMapEntry : lostZones) {
    Zone* zone = zoneMapEntry.second;
    for (auto& hostMapEntry : zone->hosts_) {
      const HostAddr& hostAddr = hostMapEntry.first;
      Host& host = hostMapEntry.second;
      for (PartitionID partId : host.parts_) {
        HostAddr dst = chooseZoneToInsert(partId);
        existTasks[partId].emplace_back(
            jobId_, spaceInfo_.spaceId_, partId, hostAddr, dst, kvstore_, adminClient_);
      }
      host.parts_.clear();
    }
    zone->calPartNum();
  }

  // all parts of lost zones have moved to active zones, then rebalance the active zones
  nebula::cpp2::ErrorCode rc =
      rebalanceActiveZones(&sortedActiveZones, &sortedZoneHosts, &existTasks);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error(apache::thrift::util::enumNameSafe(rc));
  }

  bool emty = std::find_if(existTasks.begin(),
                           existTasks.end(),
                           [](std::pair<const PartitionID, std::vector<BalanceTask>>& p) {
                             return !p.second.empty();
                           }) == existTasks.end();
  if (emty) {
    return Status::Balanced();
  }
  plan_ = std::make_unique<BalancePlan>(jobDescription_, kvstore_, adminClient_);
  std::for_each(existTasks.begin(),
                existTasks.end(),
                [this](std::pair<const PartitionID, std::vector<BalanceTask>>& p) {
                  plan_->insertTask(p.second.begin(), p.second.end());
                });
  rc = plan_->saveInStore();
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("save balance zone plan failed");
  }
  return Status::OK();
}

}  // namespace meta
}  // namespace nebula
