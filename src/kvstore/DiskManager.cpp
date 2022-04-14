/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/DiskManager.h"

DEFINE_int32(disk_check_interval_secs, 10, "interval to check free space of data path");
DEFINE_uint64(minimum_reserved_bytes, 1UL << 30, "minimum reserved bytes of each data path");

namespace nebula {
namespace kvstore {

DiskManager::DiskManager(const std::vector<std::string>& dataPaths,
                         std::shared_ptr<thread::GenericWorker> bgThread)
    : bgThread_(bgThread) {
  try {
    // atomic is not copy-constructible
    std::vector<std::atomic_uint64_t> freeBytes(dataPaths.size() + 1);
    Paths* paths = new Paths();
    paths_.store(paths);
    size_t index = 0;
    for (const auto& path : dataPaths) {
      auto absolute = boost::filesystem::absolute(path);
      if (!boost::filesystem::exists(absolute)) {
        if (!boost::filesystem::create_directories(absolute)) {
          LOG(FATAL) << folly::sformat("DataPath:{} does not exist, create failed.", path);
        }
      } else if (!boost::filesystem::is_directory(absolute)) {
        LOG(FATAL) << "DataPath is not a valid directory: " << path;
      }
      auto canonical = boost::filesystem::canonical(path);
      auto info = boost::filesystem::space(canonical);
      paths->dataPaths_.emplace_back(std::move(canonical));
      freeBytes[index++] = info.available;
    }
    freeBytes_ = std::move(freeBytes);
  } catch (boost::filesystem::filesystem_error& e) {
    LOG(FATAL) << "DataPath invalid: " << e.what();
  }
  if (bgThread_) {
    bgThread_->addRepeatTask(FLAGS_disk_check_interval_secs * 1000, &DiskManager::refresh, this);
  }
}

DiskManager::~DiskManager() {
  std::lock_guard<std::mutex> lg(lock_);
  Paths* paths = paths_.load(std::memory_order_acquire);
  folly::rcu_retire(paths, std::default_delete<Paths>());
}

StatusOr<std::vector<std::string>> DiskManager::path(GraphSpaceID spaceId) const {
  folly::rcu_reader guard;
  Paths* paths = paths_.load(std::memory_order_acquire);
  auto spaceIt = paths->partPath_.find(spaceId);
  if (spaceIt == paths->partPath_.end()) {
    return Status::Error("Space not found");
  }
  std::vector<std::string> pathsRes;
  for (const auto& partEntry : spaceIt->second) {
    pathsRes.emplace_back(partEntry.first);
  }
  return pathsRes;
}

StatusOr<std::string> DiskManager::path(GraphSpaceID spaceId, PartitionID partId) const {
  folly::rcu_reader guard;
  Paths* paths = paths_.load(std::memory_order_acquire);
  auto spaceIt = paths->partPath_.find(spaceId);
  if (spaceIt == paths->partPath_.end()) {
    return Status::Error("Space not found");
  }
  for (const auto& [path, parts] : spaceIt->second) {
    if (parts.count(partId)) {
      return path;
    }
  }
  return Status::Error("Part not found");
}

void DiskManager::addPartToPath(GraphSpaceID spaceId, PartitionID partId, const std::string& path) {
  std::lock_guard<std::mutex> lg(lock_);
  try {
    Paths* oldPaths = paths_.load(std::memory_order_acquire);
    Paths* newPaths = new Paths(*oldPaths);
    auto canonical = boost::filesystem::canonical(path);
    auto dataPath = canonical.parent_path().parent_path();
    dataPath = boost::filesystem::absolute(dataPath);
    auto iter = std::find(newPaths->dataPaths_.begin(), newPaths->dataPaths_.end(), dataPath);
    CHECK(iter != newPaths->dataPaths_.end());
    newPaths->partIndex_[spaceId][partId] = iter - newPaths->dataPaths_.begin();
    newPaths->partPath_[spaceId][canonical.string()].emplace(partId);
    paths_.store(newPaths, std::memory_order_release);
    folly::rcu_retire(oldPaths, std::default_delete<Paths>());
  } catch (boost::filesystem::filesystem_error& e) {
    LOG(FATAL) << "Invalid path: " << e.what();
  }
}

void DiskManager::removePartFromPath(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const std::string& path) {
  std::lock_guard<std::mutex> lg(lock_);
  try {
    Paths* oldPaths = paths_.load(std::memory_order_acquire);
    Paths* newPaths = new Paths(*oldPaths);
    auto canonical = boost::filesystem::canonical(path);
    auto dataPath = canonical.parent_path().parent_path();
    dataPath = boost::filesystem::absolute(dataPath);
    auto iter = std::find(newPaths->dataPaths_.begin(), newPaths->dataPaths_.end(), dataPath);
    CHECK(iter != newPaths->dataPaths_.end());
    newPaths->partIndex_[spaceId].erase(partId);
    newPaths->partPath_[spaceId][canonical.string()].erase(partId);
    paths_.store(newPaths, std::memory_order_release);
    folly::rcu_retire(oldPaths, std::default_delete<Paths>());
  } catch (boost::filesystem::filesystem_error& e) {
    LOG(FATAL) << "Invalid path: " << e.what();
  }
}

void DiskManager::getDiskParts(SpaceDiskPartsMap& diskParts) const {
  folly::rcu_reader guard;
  Paths* paths = paths_.load(std::memory_order_acquire);
  for (const auto& [space, partDiskMap] : paths->partPath_) {
    std::unordered_map<std::string, meta::cpp2::PartitionList> tmpPartPaths;
    for (const auto& [path, partitions] : partDiskMap) {
      std::vector<PartitionID> tmpPartitions;
      for (const auto& partition : partitions) {
        tmpPartitions.emplace_back(partition);
      }
      meta::cpp2::PartitionList ps;
      ps.part_list_ref() = tmpPartitions;
      tmpPartPaths[path] = ps;
    }
    diskParts.emplace(space, std::move(tmpPartPaths));
  }
}

bool DiskManager::hasEnoughSpace(GraphSpaceID spaceId, PartitionID partId) const {
  folly::rcu_reader guard;
  Paths* paths = paths_.load(std::memory_order_acquire);
  auto spaceIt = paths->partIndex_.find(spaceId);
  if (spaceIt == paths->partIndex_.end()) {
    return false;
  }
  auto partIt = spaceIt->second.find(partId);
  if (partIt == spaceIt->second.end()) {
    return false;
  }
  return freeBytes_[partIt->second].load(std::memory_order_relaxed) >= FLAGS_minimum_reserved_bytes;
}

void DiskManager::refresh() {
  // refresh the available bytes of each data path, skip the dummy path
  folly::rcu_reader guard;
  Paths* paths = paths_.load(std::memory_order_acquire);
  for (size_t i = 0; i < paths->dataPaths_.size(); i++) {
    boost::system::error_code ec;
    auto info = boost::filesystem::space(paths->dataPaths_[i], ec);
    if (!ec) {
      VLOG(2) << "Refresh filesystem info of " << paths->dataPaths_[i];
      freeBytes_[i] = info.available;
    } else {
      LOG(WARNING) << "Get filesystem info of " << paths->dataPaths_[i] << " failed";
    }
  }
}

}  // namespace kvstore
}  // namespace nebula
