/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/ZoneBalanceJobExecutor.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "common/utils/MetaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobUtils.h"

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode ZoneBalanceJobExecutor::prepare() {
  auto spaceRet = getSpaceIdFromName(paras_.back());
  if (!nebula::ok(spaceRet)) {
    LOG(ERROR) << "Can't find the space: " << paras_.back();
    return nebula::error(spaceRet);
  }
  GraphSpaceID spaceId = nebula::value(spaceRet);
  nebula::cpp2::ErrorCode rc = spaceInfo_.getInfo(spaceId, kvstore_);
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
      return status;
    }
  }
  plan_->setFinishCallBack([this](meta::cpp2::JobStatus status) {
    if (LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec()) !=
        nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Balance plan " << plan_->id() << " update meta failed";
    }
    if (status == meta::cpp2::JobStatus::FINISHED) {
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
  kvstore_->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceVal);
  meta::cpp2::SpaceDesc properties = MetaKeyUtils::parseSpace(spaceVal);
  std::vector<std::string> zones;
  for (std::string& zn : lostZones_) {
    spaceInfo_.zones_.erase(zn);
  }
  for (auto& p : spaceInfo_.zones_) {
    zones.push_back(p.first);
  }
  properties.set_zone_names(std::move(zones));
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::spaceKey(spaceInfo_.spaceId_),
                    MetaKeyUtils::spaceVal(properties));
  folly::Baton<true, std::atomic> baton;
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiPut(kDefaultSpaceId,
                          kDefaultPartId,
                          std::move(data),
                          [&baton, &ret](nebula::cpp2::ErrorCode code) {
                            if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                              ret = code;
                              LOG(ERROR) << "Can't write the kvstore, ret = "
                                         << static_cast<int32_t>(code);
                            }
                            baton.post();
                          });
  baton.wait();
  return ret;
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
  std::vector<BalanceTask> tasks;

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

  auto insertPartIntoZone = [&sortedZoneHosts](Zone* zone, PartitionID partId) -> HostAddr {
    std::vector<Host*>& sortedHosts = sortedZoneHosts[zone->zoneName_];
    sortedHosts.front()->parts_.emplace(partId);
    zone->partNum_++;
    HostAddr ha = sortedHosts.front()->ha_;
    for (size_t i = 0; i < sortedHosts.size() - 1; i++) {
      if (sortedHosts[i]->parts_.size() >= sortedHosts[i + 1]->parts_.size()) {
        std::swap(sortedHosts[i], sortedHosts[i + 1]);
      } else {
        break;
      }
    }
    return ha;
  };

  auto chooseZoneToInsert = [&insertPartIntoZone,
                             &sortedActiveZones](PartitionID partId) -> HostAddr {
    size_t index = 0;
    for (size_t i = 0; i < sortedActiveZones.size(); i++) {
      if (!sortedActiveZones[i]->partExist(partId)) {
        index = i;
        break;
      }
    }
    HostAddr ha = insertPartIntoZone(sortedActiveZones[index], partId);
    for (size_t i = index; i < sortedActiveZones.size() - 1; i++) {
      if (sortedActiveZones[i]->partNum_ >= sortedActiveZones[i + 1]->partNum_) {
        std::swap(sortedActiveZones[i], sortedActiveZones[i + 1]);
      } else {
        break;
      }
    }
    return ha;
  };

  for (auto& zoneMapEntry : lostZones) {
    Zone* zone = zoneMapEntry.second;
    for (auto& hostMapEntry : zone->hosts_) {
      for (PartitionID partId : hostMapEntry.second.parts_) {
        HostAddr dst = chooseZoneToInsert(partId);
        tasks.emplace_back(
            jobId_, spaceInfo_.spaceId_, partId, hostMapEntry.first, dst, kvstore_, adminClient_);
      }
      hostMapEntry.second.parts_.clear();
    }
    zone->calPartNum();
  }

  int32_t totalPartNum = 0;
  int32_t avgPartNum = 0;
  for (auto& z : sortedActiveZones) {
    totalPartNum += z->partNum_;
  }
  avgPartNum = totalPartNum / sortedActiveZones.size();
  int32_t remainder = totalPartNum - avgPartNum * sortedActiveZones.size();
  int32_t leftBegin = 0;
  int32_t leftEnd = 0;
  int32_t rightBegin = 0;
  int32_t rightEnd = sortedActiveZones.size();
  for (size_t i = 0; i < sortedActiveZones.size(); i++) {
    if (avgPartNum <= sortedActiveZones[i]->partNum_) {
      leftEnd = i;
      break;
    }
  }
  for (size_t i = leftEnd; i < sortedActiveZones.size(); i++) {
    if (avgPartNum < sortedActiveZones[i]->partNum_) {
      rightBegin = i;
      break;
    }
  }
  for (int32_t right = rightBegin; right < rightEnd;) {
    Zone* srcZone = sortedActiveZones[right];
    // if remainder>0 some zones will hold avgPartNum+1 patrs, we prioritise taking the right side
    // zones to hold them
    if (srcZone->partNum_ == avgPartNum + 1 && remainder) {
      right++;
      remainder--;
      continue;
    }
    if (srcZone->partNum_ == avgPartNum) {
      right++;
      continue;
    }
    std::vector<Host*>& sortedHosts = sortedZoneHosts[srcZone->zoneName_];
    int32_t hostIndex = sortedHosts.size() - 1;
    // to find a part to move,we prioritise moving parts from who has the most
    for (; hostIndex >= 0; hostIndex--) {
      std::set<PartitionID>& hostParts = sortedHosts[hostIndex]->parts_;
      PartitionID movePart = -1;
      for (PartitionID partId : hostParts) {
        bool matched = false;
        // to find a zone which does not contain the part in the left side to insert
        for (int32_t leftIndex = leftBegin; leftIndex < leftEnd; leftIndex++) {
          if (!sortedActiveZones[leftIndex]->partExist(partId)) {
            HostAddr dst = insertPartIntoZone(sortedActiveZones[leftIndex], partId);
            tasks.emplace_back(jobId_,
                               spaceInfo_.spaceId_,
                               partId,
                               sortedHosts[hostIndex]->ha_,
                               dst,
                               kvstore_,
                               adminClient_);
            movePart = partId;
            int32_t newLeftIndex = leftIndex;
            for (; newLeftIndex < leftEnd - 1; newLeftIndex++) {
              if (sortedActiveZones[newLeftIndex]->partNum_ >
                  sortedActiveZones[newLeftIndex + 1]->partNum_) {
                std::swap(sortedActiveZones[newLeftIndex], sortedActiveZones[newLeftIndex + 1]);
              } else {
                break;
              }
            }
            // if the zone's part reach the avgPartNum,is can't recieve parts any more
            if (newLeftIndex == leftEnd - 1 &&
                sortedActiveZones[newLeftIndex]->partNum_ >= avgPartNum) {
              leftEnd--;
            }
            // all zones in left side have reached avgPartNum,and now some of them will take
            // avgPartNum+1 if there still has remainder
            if (leftBegin == leftEnd) {
              leftEnd = rightBegin;
            }
            matched = true;
            break;
          }
        }
        if (matched) {
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
  if (tasks.empty()) {
    return Status::Balanced();
  }
  plan_.reset(new BalancePlan(jobDescription_, kvstore_, adminClient_));
  for (BalanceTask& task : tasks) {
    plan_->addTask(std::move(task));
  }
  nebula::cpp2::ErrorCode rc = plan_->saveInStore();
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("save balance zone plan failed");
  }
  return Status::OK();
}

}  // namespace meta
}  // namespace nebula
