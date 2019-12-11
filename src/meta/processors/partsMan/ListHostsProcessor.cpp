/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"

DECLARE_int32(expired_threshold_sec);
DEFINE_int32(removed_threshold_sec, 24 * 60 * 60,
                     "Hosts will be removed in this time if no heartbeat received");

namespace nebula {
namespace meta {

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    UNUSED(req);
    std::unordered_map<GraphSpaceID, std::string> spaceIdNameMap;
    std::vector<cpp2::HostItem> hostItems;
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto status = allHostsWithStatus(spaceIdNameMap);
        if (!status.ok()) {
            onFinished();
            return;
        }
        hostItems = std::move(status.value());
    }
    getLeaderDist(hostItems, spaceIdNameMap);
    resp_.set_hosts(std::move(hostItems));
    onFinished();
}

StatusOr<std::vector<cpp2::HostItem>> ListHostsProcessor::allHostsWithStatus(
                                    std::unordered_map<GraphSpaceID, std::string>& spaceIdNameMap) {
    std::vector<cpp2::HostItem> hostItems;

    const auto& hostPrefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No hosts";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    auto now = time::WallClock::fastNowInMilliSec();
    std::vector<std::string> removeHostsKey;
    while (iter->valid()) {
        cpp2::HostItem item;
        nebula::cpp2::HostAddr host;
        auto hostAddrPiece = iter->key().subpiece(hostPrefix.size());
        memcpy(&host, hostAddrPiece.data(), hostAddrPiece.size());
        item.set_hostAddr(host);
        HostInfo info = HostInfo::decode(iter->val());
        if (now - info.lastHBTimeInMilliSec_ < FLAGS_removed_threshold_sec * 1000) {
            if (now - info.lastHBTimeInMilliSec_ < FLAGS_expired_threshold_sec * 1000) {
                item.set_status(cpp2::HostStatus::ONLINE);
            } else {
                item.set_status(cpp2::HostStatus::OFFLINE);
            }
            hostItems.emplace_back(item);
        } else {
            removeHostsKey.emplace_back(iter->key());
        }
        iter->next();
    }

    // Get all spaces
    std::vector<GraphSpaceID> spaces;
    const auto& spacePrefix = MetaServiceUtils::spacePrefix();
    kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        return hostItems;
    }
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        spaces.emplace_back(spaceId);
        spaceIdNameMap.emplace(spaceId, MetaServiceUtils::spaceName(iter->val()));
        iter->next();
    }

    std::unordered_map<HostAddr,
                       std::unordered_map<std::string, std::vector<PartitionID>>> allParts;
    for (const auto& spaceId : spaces) {
        // get space name by space id
        auto spaceName = spaceIdNameMap[spaceId];

        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        const auto& partPrefix = MetaServiceUtils::partPrefix(spaceId);
        kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
        if (kvRet != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "List Hosts Failed: No partitions";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            return Status::Error("Cant't find any partitions");
        }
        while (iter->valid()) {
            auto key = iter->key();
            PartitionID partId;
            memcpy(&partId, key.data() + partPrefix.size(), sizeof(PartitionID));
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
        auto it = std::find_if(hostItems.begin(), hostItems.end(), [&](const auto& item) {
            return item.get_hostAddr() == hostAddr;
        });
        if (it != hostItems.end()) {
            // set default leader parts of all space to empty
            std::unordered_map<std::string, std::vector<PartitionID>> leaderParts;
            for (auto& spaceEntry : hostEntry.second) {
                leaderParts[spaceEntry.first] = {};
            }
            it->set_leader_parts(std::move(leaderParts));
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
    return hostItems;
}

void ListHostsProcessor::getLeaderDist(
                                std::vector<cpp2::HostItem>& hostItems,
                                std::unordered_map<GraphSpaceID, std::string>& spaceIdNameMap) {
    if (adminClient_ == nullptr) {
        return;
    }
    HostLeaderMap hostLeaderMap;
    auto ret = adminClient_->getLeaderDist(&hostLeaderMap).get();
    if (!ret.ok()) {
        LOG(ERROR) << "Get leader distribution failed";
        return;
    }
    for (auto& hostEntry : hostLeaderMap) {
        auto hostAddr = toThriftHost(hostEntry.first);
        auto it = std::find_if(hostItems.begin(), hostItems.end(), [&](const auto& item) {
            return item.get_hostAddr() == hostAddr;
        });

        if (it != hostItems.end()) {
            for (auto& leaderEntry : hostEntry.second) {
                // get space name by space id
                auto spaceId = leaderEntry.first;
                auto spaceIter = spaceIdNameMap.find(spaceId);
                if (spaceIter == spaceIdNameMap.end()) {
                    continue;
                }
                auto spaceName = spaceIter->second;

                it->leader_parts[spaceName] = std::move(leaderEntry.second);
            }
        }
    }
}


}  // namespace meta
}  // namespace nebula

