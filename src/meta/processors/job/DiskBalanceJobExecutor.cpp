/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/DiskBalanceJobExecutor.h"

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode DiskBalanceJobExecutor::prepare() {
  auto spaceRet = getSpaceIdFromName(paras_.back());
  if (!nebula::ok(spaceRet)) {
    LOG(ERROR) << "Can't find the space: " << paras_.back();
    return nebula::error(spaceRet);
  }
  auto spaceId = nebula::value(spaceRet);
  auto code = spaceInfo_.loadInfo(spaceId, kvstore_);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }

  host_ = HostAddr::fromString(paras_[0]);
  for (size_t i = 1; i < paras_.size() - 1; i++) {
    LOG(INFO) << "Normal path: " << paras_[i];
    auto path = paras_[i].substr(1, paras_[i].size() - 2);
    if (path[0] != '/') {
      return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
    paths_.emplace_back(path);
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status> DiskBalanceJobExecutor::executeInternal() {
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

ErrorOr<nebula::cpp2::ErrorCode, DiskParts> DiskBalanceJobExecutor::assemblePartLocation(
    HostAddr host, GraphSpaceID spaceId, int32_t& totalParts) {
  PartDiskMap* partDiskMap = new PartDiskMap();
  auto status = adminClient_->getPartsDist(host, spaceId, partDiskMap).get();
  sleep(1);
  if (!status.ok()) {
    LOG(ERROR) << "Get parts distributed failed";
    return nebula::cpp2::ErrorCode::E_RPC_FAILURE;
  }

  DiskParts partsLocation;
  for (const auto& partDisk : *partDiskMap) {
    auto partId = partDisk.first;
    auto disk = partDisk.second;

    partsLocation[disk].emplace_back(partId);
    LOG(INFO) << "Disk " << disk << " part " << partId;
    totalParts += 1;
  }
  return partsLocation;
}

Status DiskBalanceJobExecutor::buildBalancePlan() {
  int32_t totalParts = 0;
  auto locationResult = assemblePartLocation(host_, spaceInfo_.spaceId_, totalParts);
  if (!nebula::ok(locationResult)) {
    LOG(ERROR) << "Fetch hosts and parts failed";
    return Status::Error("Fetch hosts and parts failed");
  }

  auto location = nebula::value(locationResult);
  std::vector<BalanceTask> tasks;
  nebula::cpp2::ErrorCode code;
  if (dismiss_) {
    code = detachDiskBalance(host_, spaceInfo_.spaceId_, location, paths_, tasks);
  } else {
    code = additionDiskBalance(host_, spaceInfo_.spaceId_, location, paths_, totalParts, tasks);
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("build balance plan failed");
  }

  plan_.reset(new BalancePlan(jobDescription_, kvstore_, adminClient_));
  for (auto& task : tasks) {
    plan_->addTask(std::move(task));
  }

  code = plan_->saveInStore();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("save balance zone plan failed");
  }

  return Status::OK();
}

nebula::cpp2::ErrorCode DiskBalanceJobExecutor::detachDiskBalance(
    const HostAddr& host,
    GraphSpaceID spaceId,
    DiskParts& partsLocation,
    const std::vector<std::string>& paths,
    std::vector<BalanceTask>& tasks) {
  LOG(INFO) << "detachDiskBalance";
  if (partsLocation.size() == 1) {
    LOG(ERROR) << "Can't remove only one disk on " << host;
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  if (paths.empty()) {
    LOG(ERROR) << "Paths is empty";
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  std::vector<std::string> ps = paths;
  auto iter = std::unique(ps.begin(), ps.end());
  if (iter != ps.end()) {
    LOG(ERROR) << "Paths have duplicate element";
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  for (auto& path : paths) {
    auto spacePath = folly::stringPrintf("%s/nebula/%d", path.c_str(), spaceInfo_.spaceId_);
    LOG(INFO) << "Path: " << spacePath;
    auto diskIt = partsLocation.find(spacePath);
    if (diskIt == partsLocation.end()) {
      LOG(ERROR) << "Can't find " << path;
      return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
  }

  if (partsLocation.size() == paths.size()) {
    LOG(ERROR) << "Can't remove all disks on " << host;
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  auto locations = partsLocation;
  for (auto& path : paths) {
    auto spacePath = folly::stringPrintf("%s/nebula/%d", path.c_str(), spaceInfo_.spaceId_);
    auto diskIt = locations.find(spacePath);
    locations.erase(diskIt);
  }

  for (auto& path : paths) {
    auto spacePath = folly::stringPrintf("%s/nebula/%d", path.c_str(), spaceInfo_.spaceId_);
    auto& parts = partsLocation[spacePath];
    LOG(INFO) << "Path: " << spacePath << "parts size " << parts.size();
    for (auto part : parts) {
      auto sortedDisks = sortedDisksByParts(locations);
      if (sortedDisks.empty()) {
        LOG(ERROR) << "Disk is empty";
        return nebula::cpp2::ErrorCode::E_BAD_BALANCE_PLAN;
      }

      auto minPartsDisk = sortedDisks.front();
      auto targetDisk = minPartsDisk.first;
      tasks.emplace_back(
          jobId_, spaceId, part, host, spacePath, host, targetDisk, kvstore_, adminClient_);
      auto& minParts = locations[targetDisk];
      minParts.emplace_back(part);
      LOG(INFO) << "Move part " << part << " from " << spacePath << " to " << targetDisk;
    }
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode DiskBalanceJobExecutor::additionDiskBalance(
    const HostAddr& host,
    GraphSpaceID spaceId,
    DiskParts& partsLocation,
    const std::vector<std::string>& paths,
    int32_t totalParts,
    std::vector<BalanceTask>& tasks) {
  LOG(INFO) << "additionDiskBalance";
  if (totalParts == 0) {
    LOG(ERROR) << "Total partition number is zero";
    LOG(ERROR) << "Maybe not need balance";
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  if (partsLocation.empty() || paths.empty()) {
    LOG(ERROR) << "Disk partition location is empty";
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  std::vector<std::string> ps = paths;
  auto iter = std::unique(ps.begin(), ps.end());
  if (iter != ps.end()) {
    LOG(ERROR) << "Paths have duplicate element";
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  float avgLoad = static_cast<float>(totalParts) / (paths.size() + partsLocation.size());
  LOG(INFO) << "The expect avg load is " << avgLoad;
  int32_t minLoad = std::floor(avgLoad);
  int32_t maxLoad = std::ceil(avgLoad);
  LOG(INFO) << "The min load is " << minLoad << " max load is " << maxLoad;

  for (auto path : paths) {
    auto spacePath = folly::stringPrintf("%s/nebula/%d", path.c_str(), spaceInfo_.spaceId_);
    auto diskIt = partsLocation.find(spacePath);
    if (diskIt != partsLocation.end()) {
      LOG(ERROR) << "Disk " << path << " have exist";
      return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    } else {
      partsLocation.emplace(spacePath, std::vector<PartitionID>());
    }
  }

  auto sortedDisks = sortedDisksByParts(partsLocation);
  if (sortedDisks.empty()) {
    LOG(ERROR) << "Disk is empty";
    return nebula::cpp2::ErrorCode::E_BAD_BALANCE_PLAN;
  }

  auto maxPartsDisk = sortedDisks.back();
  auto minPartsDisk = sortedDisks.front();
  while (maxPartsDisk.second > maxLoad || minPartsDisk.second < minLoad) {
    std::string sourceDisk = maxPartsDisk.first;
    std::string targetDisk = minPartsDisk.first;

    auto& sourceParts = partsLocation[sourceDisk];
    auto& targetParts = partsLocation[targetDisk];
    auto partId = sourceParts.front();
    if (std::find(targetParts.begin(), targetParts.end(), partId) != targetParts.end()) {
      LOG(ERROR) << "Can't find " << partId;
      return nebula::cpp2::ErrorCode::E_BAD_BALANCE_PLAN;
    }

    targetParts.emplace_back(partId);
    sourceParts.erase(sourceParts.begin());

    LOG(INFO) << "Move Part: " << partId << " from " << sourceDisk << " to " << targetDisk;
    tasks.emplace_back(
        jobId_, spaceId, partId, host, sourceDisk, host, targetDisk, kvstore_, adminClient_);
    sortedDisks = sortedDisksByParts(partsLocation);
    maxPartsDisk = sortedDisks.back();
    minPartsDisk = sortedDisks.front();
  }
  partsLocation.clear();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::vector<std::pair<std::string, int32_t>> DiskBalanceJobExecutor::sortedDisksByParts(
    const DiskParts& partsLocation) {
  std::vector<std::pair<std::string, int32_t>> disks;
  for (auto it = partsLocation.begin(); it != partsLocation.end(); it++) {
    disks.emplace_back(it->first, it->second.size());
  }
  std::sort(
      disks.begin(), disks.end(), [](const auto& l, const auto& r) { return l.second < r.second; });
  return disks;
}

}  // namespace meta
}  // namespace nebula
