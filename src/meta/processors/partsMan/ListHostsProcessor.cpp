/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"

DECLARE_int32(expired_threshold_sec);
DECLARE_int32(heartbeat_interval_secs);
DEFINE_int32(removed_threshold_sec, 24 * 60 * 60,
                     "Hosts will be removed in this time if no heartbeat received");

namespace nebula {
namespace meta {

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    UNUSED(req);
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto spaceRet = getSpaceIdNameMap();
        if (!spaceRet.ok()) {
            onFinished();
            return;
        }
        auto status = allHostsWithStatus();
        if (!status.ok()) {
            onFinished();
            return;
        }
    }
    resp_.set_hosts(std::move(hostItems_));
    onFinished();
}

Status ListHostsProcessor::allHostsWithStatus() {
    const auto& hostPrefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No hosts";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    auto now = time::WallClock::fastNowInMilliSec();
    std::vector<std::string> removeHostsKey;
    while (iter->valid()) {
        cpp2::HostItem item;
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        item.set_hostAddr(std::move(host));
        HostInfo info = HostInfo::decode(iter->val());
        if (now - info.lastHBTimeInMilliSec_ < FLAGS_removed_threshold_sec * 1000) {
            if (now - info.lastHBTimeInMilliSec_ < FLAGS_expired_threshold_sec * 1000) {
                item.set_status(cpp2::HostStatus::ONLINE);
            } else {
                item.set_status(cpp2::HostStatus::OFFLINE);
            }
            hostItems_.emplace_back(item);
        } else {
            removeHostsKey.emplace_back(iter->key());
        }
        iter->next();
    }

    const auto& leaderPrefix = MetaServiceUtils::leaderPrefix();
    kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, leaderPrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No leaders";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    // get hosts which have send heartbeat recently
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_, FLAGS_heartbeat_interval_secs * 2);
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseLeaderKey(iter->key());
        if (std::find(activeHosts.begin(), activeHosts.end(),
                      HostAddr(host.ip, host.port)) != activeHosts.end()) {
            auto hostIt = std::find_if(hostItems_.begin(), hostItems_.end(), [&](const auto& item) {
                return item.get_hostAddr() == host;
            });
            if (hostIt != hostItems_.end()) {
                LeaderParts leaderParts = MetaServiceUtils::parseLeaderVal(iter->val());
                hostIt->set_leader_parts(getLeaderPartsWithSpaceName(leaderParts));
            }
        }
        iter->next();
    }

    std::unordered_map<HostAddr,
                       std::unordered_map<std::string, std::vector<PartitionID>>> allParts;
    for (const auto& spaceId : spaceIds_) {
        // get space name by space id
        const auto& spaceName = spaceIdNameMap_[spaceId];

        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        const auto& partPrefix = MetaServiceUtils::partPrefix(spaceId);
        kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
        if (kvRet != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "List Hosts Failed: No partitions";
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            return Status::Error("Can't find any partitions");
        }
        while (iter->valid()) {
            PartitionID partId = MetaServiceUtils::parsePartKeyPartId(iter->key());
            auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
            for (auto& host : partHosts) {
                hostParts[HostAddr(host.ip, host.port)].emplace_back(partId);
            }
            iter->next();
        }

        for (const auto& hostEntry : hostParts) {
            allParts[hostEntry.first][spaceName] = std::move(hostEntry.second);
        }
    }

    for (const auto& hostEntry : allParts) {
        auto hostAddr = toThriftHost(hostEntry.first);
        auto it = std::find_if(hostItems_.begin(), hostItems_.end(), [&](const auto& item) {
            return item.get_hostAddr() == hostAddr;
        });
        if (it != hostItems_.end()) {
            it->set_all_parts(std::move(hostEntry.second));
        }
    }

    // Remove hosts that long time at OFFLINE status
    if (!removeHostsKey.empty()) {
        kvstore_->asyncMultiRemove(kDefaultSpaceId,
                                   kDefaultPartId,
                                   std::move(removeHostsKey),
                                   [] (kvstore::ResultCode code) {
                if (code != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Async remove long time offline hosts failed: " << code;
                }
            });
    }
    return Status::OK();
}

Status ListHostsProcessor::getSpaceIdNameMap() {
    // Get all spaces
    const auto& spacePrefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No space found";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        spaceIds_.emplace_back(spaceId);
        spaceIdNameMap_.emplace(spaceId, MetaServiceUtils::spaceName(iter->val()));
        iter->next();
    }
    return Status::OK();
}

std::unordered_map<std::string, std::vector<PartitionID>>
ListHostsProcessor::getLeaderPartsWithSpaceName(const LeaderParts& leaderParts) {
    std::unordered_map<std::string, std::vector<PartitionID>> result;
    for (const auto& spaceEntry : leaderParts) {
        GraphSpaceID spaceId = spaceEntry.first;
        auto iter = spaceIdNameMap_.find(spaceId);
        if (iter != spaceIdNameMap_.end()) {
            // ignore space not exists
            result.emplace(iter->second, std::move(spaceEntry.second));
        }
    }
    return result;
}

}  // namespace meta
}  // namespace nebula

