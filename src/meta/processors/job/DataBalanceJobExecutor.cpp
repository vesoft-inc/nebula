/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/DataBalanceJobExecutor.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "common/utils/MetaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobUtils.h"

namespace nebula {
namespace meta {

folly::Future<Status> DataBalanceJobExecutor::executeInternal() {
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
    executorOnFinished_(status);
  });
  plan_->invoke();
  return Status::OK();
}

Status DataBalanceJobExecutor::buildBalancePlan() {
  std::map<std::string, std::vector<Host*>> lostZoneHost;
  std::map<std::string, std::vector<Host*>> activeSortedHost;
  for (auto& zoneMapEntry : spaceInfo_.zones_) {
    for (auto& hostMapEntry : zoneMapEntry.second.hosts_) {
      activeSortedHost[zoneMapEntry.first].push_back(&hostMapEntry.second);
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
  plan_.reset(new BalancePlan(jobDescription_, kvstore_, adminClient_));
  // move parts of lost hosts to active hosts in the same zone
  for (auto& zoneHostEntry : lostZoneHost) {
    std::vector<Host*>& hvec = activeSortedHost[zoneHostEntry.first];
    for (Host* host : zoneHostEntry.second) {
      for (PartitionID partId : host->parts_) {
        Host* dstHost = hvec.front();
        dstHost->parts_.insert(partId);
        plan_->addTask(BalanceTask(jobId_,
                                   spaceInfo_.spaceId_,
                                   partId,
                                   host->host_,
                                   dstHost->host_,
                                   kvstore_,
                                   adminClient_));
        for (size_t i = 0; i < hvec.size() - 1; i++) {
          if (hvec[i]->parts_.size() > hvec[i + 1]->parts_.size()) {
            std::swap(hvec[i], hvec[i + 1]);
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
  auto balanceHostVec = [this](std::vector<Host*>& hostVec) -> std::vector<BalanceTask> {
    size_t totalPartNum = 0;
    size_t avgPartNum = 0;
    for (Host* h : hostVec) {
      totalPartNum += h->parts_.size();
    }
    if (hostVec.size() == 0) {
      LOG(ERROR) << "rebalance error: zone has no host";
      return {};
    }
    avgPartNum = totalPartNum / hostVec.size();
    size_t remainder = totalPartNum - avgPartNum * hostVec.size();
    size_t leftBegin = 0;
    size_t leftEnd = 0;
    size_t rightBegin = 0;
    size_t rightEnd = hostVec.size();
    std::vector<BalanceTask> tasks;
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
      tasks.emplace_back(jobId_,
                         spaceInfo_.spaceId_,
                         partId,
                         srcHost->host_,
                         hostVec[leftBegin]->host_,
                         kvstore_,
                         adminClient_);
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
    return tasks;
  };
  for (auto& pair : activeSortedHost) {
    std::vector<Host*>& hvec = pair.second;
    std::vector<BalanceTask> tasks = balanceHostVec(hvec);
    for (BalanceTask& task : tasks) {
      plan_->addTask(std::move(task));
    }
  }
  if (plan_->tasks().empty()) {
    return Status::Balanced();
  }
  nebula::cpp2::ErrorCode rc = plan_->saveInStore();
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("save balance zone plan failed");
  }
  return Status::OK();
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::stop() {
  stopped_ = true;
  plan_->stop();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode DataBalanceJobExecutor::prepare() {
  auto spaceRet = getSpaceIdFromName(paras_.back());
  if (!nebula::ok(spaceRet)) {
    LOG(ERROR) << "Can't find the space: " << paras_.back();
    return nebula::error(spaceRet);
  }
  GraphSpaceID spaceId = nebula::value(spaceRet);
  nebula::cpp2::ErrorCode rc = spaceInfo_.loadInfo(spaceId, kvstore_);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }
  lostHosts_.reserve(paras_.size() - 1);
  for (size_t i = 0; i < paras_.size() - 1; i++) {
    lostHosts_.emplace_back(HostAddr::fromString(paras_[i]));
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
