/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/BalanceJobExecutor.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "common/utils/MetaKeyUtils.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobUtils.h"

namespace nebula {
namespace meta {
BalanceJobExecutor::BalanceJobExecutor(JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       const std::vector<std::string>& paras)
    : MetaJobExecutor(jobId, kvstore, adminClient, paras) {}

bool BalanceJobExecutor::check() {
  return !paras_.empty();
}

nebula::cpp2::ErrorCode BalanceJobExecutor::prepare() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::stop() {
  stopped_ = true;
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::recovery() {
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
    LOG(INFO) << "Can't access kvstore, ret = " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto optJobRet = JobDescription::makeJobDescription(jobKey, value);
  auto optJob = nebula::value(optJobRet);
  plan_.reset(new BalancePlan(optJob, kvstore_, adminClient_));
  plan_->setFinishCallBack([this](meta::cpp2::JobStatus status) {
    folly::SharedMutex::WriteHolder holder(LockUtils::lock());
    auto ret = updateLastTime();
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Balance plan " << plan_->id() << " update meta failed";
    }
    executorOnFinished_(status);
  });
  auto recRet = plan_->recovery();
  if (recRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    plan_.reset(nullptr);
    return recRet;
  }
  plan_->saveInStore();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::finish(bool) {
  plan_.reset(nullptr);
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

void BalanceJobExecutor::insertOneTask(
    const BalanceTask& task, std::map<PartitionID, std::vector<BalanceTask>>* existTasks) {
  std::vector<BalanceTask>& taskVec = existTasks->operator[](task.getPartId());
  if (taskVec.empty()) {
    taskVec.emplace_back(task);
  } else {
    for (auto it = taskVec.begin(); it != taskVec.end(); it++) {
      if (task.getDstHost() == it->getSrcHost() && task.getSrcHost() == it->getDstHost()) {
        taskVec.erase(it);
        return;
      } else if (task.getDstHost() == it->getSrcHost()) {
        BalanceTask newTask(task);
        newTask.setDstHost(it->getDstHost());
        taskVec.erase(it);
        insertOneTask(newTask, existTasks);
        return;
      } else if (task.getSrcHost() == it->getDstHost()) {
        BalanceTask newTask(task);
        newTask.setSrcHost(it->getSrcHost());
        taskVec.erase(it);
        insertOneTask(newTask, existTasks);
        return;
      } else {
        continue;
      }
    }
    taskVec.emplace_back(task);
  }
}

nebula::cpp2::ErrorCode SpaceInfo::loadInfo(GraphSpaceID spaceId, kvstore::KVStore* kvstore) {
  spaceId_ = spaceId;
  std::string spaceKey = MetaKeyUtils::spaceKey(spaceId);
  std::string spaceVal;
  auto rc = kvstore->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceVal);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get space info " << spaceId
              << " failed, error: " << apache::thrift::util::enumNameSafe(rc);
    return rc;
  }
  meta::cpp2::SpaceDesc properties = MetaKeyUtils::parseSpace(spaceVal);
  name_ = properties.get_space_name();
  replica_ = properties.get_replica_factor();
  const std::vector<std::string>& zones = properties.get_zone_names();
  for (const std::string& zoneName : zones) {
    std::string zoneValue;
    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    auto retCode = kvstore->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Get zone " << zoneName
                << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }
    std::vector<HostAddr> hosts = MetaKeyUtils::parseZoneHosts(std::move(zoneValue));
    Zone zone(zoneName);
    for (HostAddr& ha : hosts) {
      zone.hosts_.emplace(ha, Host(ha));
    }
    zones_.emplace(zoneName, zone);
  }
  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Access kvstore failed, spaceId " << spaceId << " "
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  for (; iter->valid(); iter->next()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    std::vector<HostAddr> partHosts = MetaKeyUtils::parsePartVal(iter->val());
    for (HostAddr& ha : partHosts) {
      for (auto& [zn, zone] : zones_) {
        auto it = zone.hosts_.find(ha);
        if (it != zone.hosts_.end()) {
          it->second.parts_.emplace(partId);
        }
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

int32_t Zone::calPartNum() {
  int32_t num = 0;
  for (auto& p : hosts_) {
    num += p.second.parts_.size();
  }
  partNum_ = num;
  return partNum_;
}

bool Zone::partExist(PartitionID partId) {
  for (auto& p : hosts_) {
    if (p.second.parts_.count(partId)) {
      return true;
    }
  }
  return false;
}

bool SpaceInfo::hasHost(const HostAddr& ha) {
  for (auto p : zones_) {
    if (p.second.hasHost(ha)) {
      return true;
    }
  }
  return false;
}

}  // namespace meta
}  // namespace nebula
