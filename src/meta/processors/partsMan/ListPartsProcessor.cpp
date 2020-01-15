/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/partsMan/ListPartsProcessor.h"
#include "meta/processors/admin/AdminClient.h"

DECLARE_int32(expired_threshold_sec);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void ListPartsProcessor::process(const cpp2::ListPartsReq& req) {
    spaceId_ = req.get_space_id();
    partIds_ = req.get_part_ids();
    std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>> partHostsMap;

    if (!partIds_.empty()) {
        // Only show the specified parts
        showAllParts_ = false;
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        for (const auto& partId : partIds_) {
            auto partKey = MetaServiceUtils::partKey(spaceId_, partId);
            std::string value;
            auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, partKey, &value);
            if (retCode == kvstore::ResultCode::SUCCEEDED) {
                partHostsMap[partId] = MetaServiceUtils::parsePartVal(value);
            }
        }
    } else {
        // Show all parts
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto status = getAllParts();
        if (!status.ok()) {
            onFinished();
            return;
        }
        partHostsMap = std::move(status.value());
    }
    std::vector<cpp2::PartItem> partItems;
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_);
    for (auto& partEntry : partHostsMap) {
        cpp2::PartItem partItem;
        partItem.set_part_id(partEntry.first);
        partItem.set_peers(std::move(partEntry.second));
        std::vector<nebula::cpp2::HostAddr> losts;
        for (auto& host : partItem.get_peers()) {
            if (std::find(activeHosts.begin(), activeHosts.end(),
                          HostAddr(host.ip, host.port)) == activeHosts.end()) {
                losts.emplace_back();
                losts.back().set_ip(host.ip);
                losts.back().set_port(host.port);
            }
        }
        partItem.set_losts(std::move(losts));
        partIdIndex_.emplace(partEntry.first, partItems.size());
        partItems.emplace_back(std::move(partItem));
    }
    if (partItems.size() != partHostsMap.size()) {
        LOG(ERROR) << "Maybe lost some partitions!";
    }
    getLeaderDist(partItems);
    resp_.set_parts(std::move(partItems));
    onFinished();
}


StatusOr<std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>>>
ListPartsProcessor::getAllParts() {
    std::unordered_map<PartitionID, std::vector<nebula::cpp2::HostAddr>> partHostsMap;

    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(spaceId_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Parts Failed: No parts";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        std::vector<nebula::cpp2::HostAddr> partHosts = MetaServiceUtils::parsePartVal(iter->val());
        partHostsMap.emplace(partId, std::move(partHosts));
        iter->next();
    }

    return partHostsMap;
}


void ListPartsProcessor::getLeaderDist(std::vector<cpp2::PartItem>& partItems) {
    const auto& hostPrefix = MetaServiceUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        return;
    }

    // get hosts which have send heartbeat recently
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_, FLAGS_heartbeat_interval_secs + 1);
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseLeaderKey(iter->key());
        if (std::find(activeHosts.begin(), activeHosts.end(),
                      HostAddr(host.ip, host.port)) != activeHosts.end()) {
            LeaderParts leaderParts = MetaServiceUtils::parseLeaderVal(iter->val());
            const auto& partIds = leaderParts[spaceId_];
            for (const auto& partId : partIds) {
                auto partIt = partIdIndex_.find(partId);
                if (partIt != partIdIndex_.end()) {
                    partItems[partIt->second].set_leader(host);
                }
            }
        }
        iter->next();
    }
}

}  // namespace meta
}  // namespace nebula

