/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/DataBalanceJobExecutor.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include <memory>

#include "common/utils/MetaKeyUtils.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace meta {

folly::Future<nebula::cpp2::ErrorCode> DataBalanceJobExecutor::executeInternal() {
  if (plan_ == nullptr) {
    Status status = buildBalancePlan();
    if (status != Status::OK()) {
      if (status == Status::Balanced()) {
        jobDescription_.setStatus(meta::cpp2::JobStatus::FINISHED, true);
        return nebula::cpp2::ErrorCode::SUCCEEDED;
      }
      LOG(INFO) << "Build balance plan: " << status;
      return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }
  }

  return plan_->invoke().thenValue([this](meta::cpp2::JobStatus status) mutable {
    folly::SharedMutex::WriteHolder holder(LockUtils::lock());
    auto ret = updateLastTime();
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Balance plan " << plan_->id() << " update meta failed";
    }
    jobDescription_.setStatus(status, true);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  });
}

Status DataBalanceJobExecutor::buildBalancePlan() {
  std::map<std::string, std::vector<Host*>> lostZoneHost;
  std::map<std::string, std::vector<Host*>> activeSortedHost;
  for (auto& zoneMapEntry : spaceInfo_.zones_) {
    const auto& zoneName = zoneMapEntry.first;
    auto& zone = zoneMapEntry.second;
    for (auto& [addr, host] : zone.hosts_) {
      activeSortedHost[zoneName].push_back(&host);
    }
  }
  for (HostAddr host : lostHosts_) {
    if (!spaceInfo_.hasHost(host)) {
      return Status::Error(
          "Host %s does not belong to space %d", host.toString().c_str(), spaceInfo_.spaceId_);
    }
    for (auto& zoneMapEntry : spaceInfo_.zones_) {
      auto it = zoneMapEntry.second.hosts_.find(host);
      if (it != zoneMapEntry.second.hosts_.end()) {
        lostZoneHost[zoneMapEntry.first].push_back(&it->second);
        std::vector<Host*>& hvec = activeSortedHost[zoneMapEntry.first];
        hvec.erase(std::find(hvec.begin(), hvec.end(), &it->second));
        break;
      }
    }
  }
  for (auto& hostMapEntry : activeSortedHost) {
    std::vector<Host*>& hvec = hostMapEntry.second;
    std::sort(hvec.begin(), hvec.end(), [](Host*& l, Host*& r) -> bool {
      return l->parts_.size() < r->parts_.size();
    });
  }
  std::map<PartitionID, std::vector<BalanceTask>> existTasks;
  // move parts of lost hosts to active hosts in the same zone
  for (auto& zoneHostEntry : lostZoneHost) {
    const std::string& zoneName = zoneHostEntry.first;
    std::vector<Host*>& lostHostVec = zoneHostEntry.second;
    std::vector<Host*>& activeVec = activeSortedHost[zoneName];
    if (activeVec.size() == 0) {
      return Status::Error("zone %s has no host", zoneName.c_str());
    }
    for (Host* host : lostHostVec) {
      for (PartitionID partId : host->parts_) {
        Host* dstHost = activeVec.front();
        dstHost->parts_.insert(partId);
        existTasks[partId].emplace_back(jobId_,
                                        spaceInfo_.spaceId_,
                                        partId,
                                        host->host_,
                                        dstHost->host_,
                                        kvstore_,
                                        adminClient_);
        for (size_t i = 0; i < activeVec.size() - 1; i++) {
          if (activeVec[i]->parts_.size() > activeVec[i + 1]->parts_.size()) {
            std::swap(activeVec[i], activeVec[i + 1]);
          } else {
            break;
          }
        }
      }
      host->parts_.clear();
    }
  }
  lostZoneHost.clear();
  // rebalance for hosts in a zone
  auto balanceHostVec = [this, &existTasks](std::vector<Host*>& hostVec) {
    size_t totalPartNum = 0;
    size_t avgPartNum = 0;
    for (Host* h : hostVec) {
      totalPartNum += h->parts_.size();
    }
    if (hostVec.size() == 0) {
      LOG(INFO) << "rebalance error: zone has no host";
      return;
    }
    avgPartNum = totalPartNum / hostVec.size();
    size_t remainder = totalPartNum - avgPartNum * hostVec.size();
    size_t leftBegin = 0;
    size_t leftEnd = 0;
    size_t rightBegin = 0;
    size_t rightEnd = hostVec.size();
    for (size_t i = 0; i < hostVec.size(); i++) {
      if (avgPartNum <= hostVec[i]->parts_.size()) {
        leftEnd = i;
        break;
      }
    }
    for (size_t i = leftEnd; i < hostVec.size(); i++) {
      rightBegin = i;
      if (avgPartNum < hostVec[i]->parts_.size()) {
        break;
      }
    }
    for (size_t right = rightBegin; right < rightEnd;) {
      Host* srcHost = hostVec[right];
      if (srcHost->parts_.size() == avgPartNum + 1 && remainder) {
        right++;
        remainder--;
        continue;
      }
      if (srcHost->parts_.size() == avgPartNum) {
        right++;
        continue;
      }
      PartitionID partId = *(srcHost->parts_.begin());
      hostVec[leftBegin]->parts_.insert(partId);
      srcHost->parts_.erase(partId);
      insertOneTask(BalanceTask(jobId_,
                                spaceInfo_.spaceId_,
                                partId,
                                srcHost->host_,
                                hostVec[leftBegin]->host_,
                                kvstore_,
                                adminClient_),
                    &existTasks);
      size_t leftIndex = leftBegin;
      for (; leftIndex < leftEnd - 1; leftIndex++) {
        if (hostVec[leftIndex]->parts_.size() > hostVec[leftIndex + 1]->parts_.size()) {
          std::swap(hostVec[leftIndex], hostVec[leftIndex + 1]);
        } else {
          break;
        }
      }
      if (leftIndex == leftEnd - 1 && hostVec[leftIndex]->parts_.size() >= avgPartNum) {
        leftEnd--;
      }
      if (leftBegin == leftEnd) {
        leftEnd = rightBegin;
      }
    }
  };
  for (auto& pair : activeSortedHost) {
    std::vector<Host*>& hvec = pair.second;
    balanceHostVec(hvec);
  }
  bool empty = std::find_if(existTasks.begin(),
                            existTasks.end(),
                            [](std::pair<const PartitionID, std::vector<BalanceTask>>& p) {
                              return !p.second.empty();
                            }) == existTasks.end();
  if (empty) {
    return Status::Balanced();
  }
  plan_ = std::make_unique<BalancePlan>(jobDescription_, kvstore_, adminClient_);
  std::for_each(existTasks.begin(),
                existTasks.end(),
                [this](std::pair<const PartitionID, std::vector<BalanceTask>>& p) {
                  plan_->insertTask(p.second.begin(), p.second.end());
                });
  nebula::cpp2::ErrorCode rc = plan_->saveInStore();
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("save balance plan failed");
  }
  return Status::OK();
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::stop() {
  stopped_ = true;
  plan_->stop();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::prepare() {
  auto spaceRet = spaceExist();
  if (spaceRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't find the space, spaceId " << space_;
    return spaceRet;
  }

  nebula::cpp2::ErrorCode rc = spaceInfo_.loadInfo(space_, kvstore_);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }
  lostHosts_.reserve(paras_.size());
  for (size_t i = 0; i < paras_.size(); i++) {
    lostHosts_.emplace_back(HostAddr::fromString(paras_[i]));
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
