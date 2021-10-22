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
  auto rc = spaceInfo_.loadInfo(spaceId, kvstore_);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return rc;
  }

  auto size = paras_.size();
  host_ = HostAddr::fromString(paras_[size - 2]);

  paths_.reserve(size - 1);
  for (size_t i = 0; i < paras_.size() - 2; i++) {
    paths_.emplace_back(paras_[i]);
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

Status DiskBalanceJobExecutor::buildBalancePlan() {
  return Status::OK();
}

nebula::cpp2::ErrorCode DiskBalanceJobExecutor::detachDiskBalance(
    const HostAddr& host,
    GraphSpaceID spaceId,
    DiskParts& partsLocation,
    const std::vector<std::string>& paths,
    std::vector<BalanceTask>& tasks) {
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
    LOG(INFO) << "Path: " << path;
    auto diskIt = partsLocation.find(path);
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
    auto diskIt = locations.find(path);
    locations.erase(diskIt);
  }

  for (auto& path : paths) {
    auto& parts = partsLocation[path];
    LOG(INFO) << "Path: " << path << "parts size " << parts.size();
    for (auto part : parts) {
      auto sortedDisks = sortedDisksByParts(locations);
      if (sortedDisks.empty()) {
        LOG(ERROR) << "Disk is empty";
        return nebula::cpp2::ErrorCode::E_BAD_BALANCE_PLAN;
      }

      auto minPartsDisk = sortedDisks.front();
      auto targetDisk = minPartsDisk.first;
      tasks.emplace_back(
          jobId_, spaceId, part, host, path, host, targetDisk, kvstore_, adminClient_);
      auto& minParts = locations[targetDisk];
      minParts.emplace_back(part);
      LOG(INFO) << "Move part " << part << " from " << path << " to " << targetDisk;
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
    auto diskIt = partsLocation.find(path);
    if (diskIt != partsLocation.end()) {
      LOG(ERROR) << "Disk " << path << " have exist";
      return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    } else {
      partsLocation.emplace(path, std::vector<PartitionID>());
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
