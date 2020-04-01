/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "kvstore/PartManager.h"

namespace nebula {
namespace kvstore {

PartsMap MemPartManager::parts(const HostAddr& hostAddr) {
    UNUSED(hostAddr);
    return partsMap_;
}

StatusOr<PartMeta> MemPartManager::partMeta(GraphSpaceID spaceId, PartitionID partId) {
    auto it = partsMap_.find(spaceId);
    if (it == partsMap_.end()) {
        return Status::Error("Space not found, spaceid: %d", spaceId);
    }
    auto partIt = it->second.find(partId);
    if (partIt == it->second.end()) {
        return Status::Error(
            "Part not found in MemPartManager, spaceid: %d, partId: %d ", spaceId, partId);
    }
    return partIt->second;
}

Status MemPartManager::partExist(const HostAddr&, GraphSpaceID spaceId, PartitionID partId) {
    auto it = partsMap_.find(spaceId);
    if (it != partsMap_.end()) {
        auto partIt = it->second.find(partId);
        if (partIt != it->second.end()) {
            return Status::OK();
        } else {
            return Status::PartNotFound();
        }
    }
    return Status::SpaceNotFound();
}

MetaServerBasedPartManager::MetaServerBasedPartManager(HostAddr host, meta::MetaClient *client)
    : localHost_(std::move(host)) {
    client_ = client;
    CHECK_NOTNULL(client_);
    client_->registerListener(this);
}

MetaServerBasedPartManager::~MetaServerBasedPartManager() {
    VLOG(3) << "~MetaServerBasedPartManager";
    if (nullptr != client_) {
        client_->unRegisterListener();
        client_ = nullptr;
    }
}

PartsMap MetaServerBasedPartManager::parts(const HostAddr& hostAddr) {
    return client_->getPartsMapFromCache(hostAddr);
}

StatusOr<PartMeta> MetaServerBasedPartManager::partMeta(GraphSpaceID spaceId, PartitionID partId) {
    return client_->getPartMetaFromCache(spaceId, partId);
}

Status MetaServerBasedPartManager::partExist(const HostAddr& host,
                                             GraphSpaceID spaceId,
                                             PartitionID partId) {
    return client_->checkPartExistInCache(host, spaceId, partId);
}

Status MetaServerBasedPartManager::spaceExist(const HostAddr& host,
                                              GraphSpaceID spaceId) {
    return client_->checkSpaceExistInCache(host, spaceId);
}

void MetaServerBasedPartManager::onSpaceAdded(GraphSpaceID spaceId) {
    if (handler_ != nullptr) {
        handler_->addSpace(spaceId);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onSpaceRemoved(GraphSpaceID spaceId) {
    if (handler_ != nullptr) {
        handler_->removeSpace(spaceId);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onSpaceOptionUpdated(
        GraphSpaceID spaceId,
        const std::unordered_map<std::string, std::string>& options) {
    static std::unordered_set<std::string> supportedOpt = {
        "disable_auto_compactions",
        "max_write_buffer_number",
        "write_buffer_size",
        "level0_file_num_compaction_trigger",
        "level0_slowdown_writes_trigger",
        "level0_stop_writes_trigger",
        "target_file_size_base",
        "target_file_size_multiplier",
        "max_bytes_for_level_base",
        "max_bytes_for_level_multiplier",
    };
    static std::unordered_set<std::string> supportedDbOpt = {
        "max_total_wal_size",
        "delete_obsolete_files_period_micros",
        "max_background_jobs",
        "stats_dump_period_sec",
        "compaction_readahead_size",
        "writable_file_max_buffer_size",
        "bytes_per_sync",
        "wal_bytes_per_sync",
        "delayed_write_rate",
        "avoid_flush_during_shutdown",
        "max_open_files"
    };

    std::unordered_map<std::string, std::string> opt;
    std::unordered_map<std::string, std::string> dbOpt;
    for (const auto& option : options) {
        if (supportedOpt.find(option.first) != supportedOpt.end()) {
            opt[option.first] = option.second;
        } else if (supportedDbOpt.find(option.first) != supportedDbOpt.end()) {
            dbOpt[option.first] = option.second;
        }
    }

    if (handler_ != nullptr) {
        if (!opt.empty()) {
            handler_->updateSpaceOption(spaceId, opt, false);
        }
        if (!dbOpt.empty()) {
            handler_->updateSpaceOption(spaceId, dbOpt, true);
        }
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onPartAdded(const PartMeta& partMeta) {
    if (handler_ != nullptr) {
        handler_->addPart(partMeta.spaceId_, partMeta.partId_, false);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onPartRemoved(GraphSpaceID spaceId, PartitionID partId) {
    if (handler_ != nullptr) {
        handler_->removePart(spaceId, partId);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

void MetaServerBasedPartManager::onPartUpdated(const PartMeta& partMeta) {
    UNUSED(partMeta);
}

void MetaServerBasedPartManager::fetchLeaderInfo(
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>>& leaderIds) {
    if (handler_ != nullptr) {
        handler_->allLeader(leaderIds);
    } else {
        VLOG(1) << "handler_ is nullptr!";
    }
}

}  // namespace kvstore
}  // namespace nebula
