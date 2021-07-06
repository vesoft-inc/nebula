/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
        size_t index = 0;
        for (const auto& path : dataPaths) {
            auto absolute = boost::filesystem::absolute(path);
            if (!boost::filesystem::exists(absolute)) {
                boost::filesystem::create_directories(absolute);
            } else if (!boost::filesystem::is_directory(absolute)) {
                LOG(FATAL) << "DataPath is not a valid directory: " << path;
            }
            auto canonical = boost::filesystem::canonical(path);
            auto info = boost::filesystem::space(canonical);
            dataPaths_.emplace_back(std::move(canonical));
            freeBytes[index++] = info.available;
        }
        freeBytes_ = std::move(freeBytes);
    } catch (boost::filesystem::filesystem_error& e) {
        LOG(FATAL) << "DataPath invalid: " << e.what();
    }
    if (bgThread_) {
        bgThread_->addRepeatTask(
            FLAGS_disk_check_interval_secs * 1000, &DiskManager::refresh, this);
    }
}

StatusOr<std::vector<std::string>> DiskManager::path(GraphSpaceID spaceId) {
    auto spaceIt = partPath_.find(spaceId);
    if (spaceIt == partPath_.end()) {
        return Status::Error("Space not found");
    }
    std::vector<std::string> paths;
    for (const auto& partEntry : spaceIt->second) {
        paths.emplace_back(partEntry.first);
    }
    return paths;
}

StatusOr<std::string> DiskManager::path(GraphSpaceID spaceId, PartitionID partId) {
    auto spaceIt = partPath_.find(spaceId);
    if (spaceIt == partPath_.end()) {
        return Status::Error("Space not found");
    }
    for (const auto& [path, parts] : spaceIt->second) {
        if (parts.count(partId)) {
            return path;
        }
    }
    return Status::Error("Part not found");
}

void DiskManager::addPartToPath(GraphSpaceID spaceId,
                                PartitionID partId,
                                const std::string& path) {
    std::lock_guard<std::mutex> lg(lock_);
    try {
        auto canonical = boost::filesystem::canonical(path);
        auto dataPath = canonical.parent_path().parent_path();
        auto iter = std::find(dataPaths_.begin(), dataPaths_.end(), dataPath);
        CHECK(iter != dataPaths_.end());
        partIndex_[spaceId][partId] = iter - dataPaths_.begin();
        partPath_[spaceId][canonical.string()].emplace(partId);
    } catch (boost::filesystem::filesystem_error& e) {
        LOG(FATAL) << "Invalid path: " << e.what();
    }
}

void DiskManager::removePartFromPath(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const std::string& path) {
    std::lock_guard<std::mutex> lg(lock_);
    try {
        auto canonical = boost::filesystem::canonical(path);
        auto dataPath = canonical.parent_path().parent_path();
        auto iter = std::find(dataPaths_.begin(), dataPaths_.end(), dataPath);
        CHECK(iter != dataPaths_.end());
        partIndex_[spaceId].erase(partId);
        partPath_[spaceId][canonical.string()].erase(partId);
    } catch (boost::filesystem::filesystem_error& e) {
        LOG(FATAL) << "Invalid path: " << e.what();
    }
}

StatusOr<PartDiskMap> DiskManager::partDist(GraphSpaceID spaceId) {
    auto spaceIt = partPath_.find(spaceId);
    if (spaceIt == partPath_.end()) {
        return Status::Error("Space not found");
    }
    return spaceIt->second;
}

bool DiskManager::hasEnoughSpace(GraphSpaceID spaceId, PartitionID partId) {
    auto spaceIt = partIndex_.find(spaceId);
    if (spaceIt == partIndex_.end()) {
        return false;
    }
    auto partIt = spaceIt->second.find(partId);
    if (partIt == spaceIt->second.end()) {
        return false;
    }
    return freeBytes_[partIt->second].load(std::memory_order_relaxed) >=
           FLAGS_minimum_reserved_bytes;
}

void DiskManager::refresh() {
    // refresh the available bytes of each data path, skip the dummy path
    for (size_t i = 0; i < dataPaths_.size(); i++) {
        boost::system::error_code ec;
        auto info = boost::filesystem::space(dataPaths_[i], ec);
        if (!ec) {
            VLOG(1) << "Refresh filesystem info of " << dataPaths_[i];
            freeBytes_[i] = info.available;
        } else {
            LOG(WARNING) << "Get filesystem info of " << dataPaths_[i] << " failed";
        }
    }
}

}  // namespace kvstore
}  // namespace nebula
