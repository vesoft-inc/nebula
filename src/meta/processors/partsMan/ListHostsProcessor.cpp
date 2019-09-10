/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"

DECLARE_int32(expired_threshold_sec);

namespace nebula {
namespace meta {

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto ret = allHostsWithStatus();
    if (!ret.ok()) {
        onFinished();
        return;
    }
    resp_.set_hosts(std::move(ret.value()));
    onFinished();
}

StatusOr<std::vector<cpp2::HostItem>> ListHostsProcessor::allHostsWithStatus() {
    std::vector<cpp2::HostItem> hostItems;

    const auto& hostPrefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No hosts";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    auto now = time::WallClock::fastNowInSec();
    while (iter->valid()) {
        cpp2::HostItem item;
        nebula::cpp2::HostAddr host;
        auto hostAddrPiece = iter->key().subpiece(hostPrefix.size());
        memcpy(&host, hostAddrPiece.data(), hostAddrPiece.size());
        item.set_hostAddr(host);
        HostInfo info = HostInfo::decode(iter->val());
        if (now - info.lastHBTimeInSec_ < FLAGS_expired_threshold_sec) {
            item.set_status(cpp2::HostStatus::ONLINE);
        } else {
            item.set_status(cpp2::HostStatus::OFFLINE);
        }
        hostItems.emplace_back(item);
        iter->next();
    }

    // Get all spaces
    std::vector<GraphSpaceID> spaces;
    std::unordered_map<GraphSpaceID, std::string> spaceIdName;
    const auto& spacePrefix = MetaServiceUtils::spacePrefix();
    kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        return hostItems;
    }
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        spaces.emplace_back(spaceId);
        spaceIdName.emplace(spaceId, MetaServiceUtils::spaceName(iter->val()));
        iter->next();
    }

    std::unordered_map<HostAddr,
                       std::unordered_map<std::string, std::vector<PartitionID>>> allParts;
    for (const auto& spaceId : spaces) {
        auto it = spaceIdName.find(spaceId);
        if (it == spaceIdName.end()) {
            continue;
        }
        auto spaceName = it->second;
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
            it->set_all_parts(std::move(hostEntry.second));
        }
    }

    if (adminClient_  == nullptr) {
        return hostItems;
    }
    HostLeaderMap hostLeaderMap;
    auto ret = adminClient_->getLeaderDist(&hostLeaderMap).get();
    if (!ret.ok()) {
        LOG(ERROR) << "Get leader distribution failed";
        return hostItems;
    }
    for (auto& hostEntry : hostLeaderMap) {
        auto hostAddr = toThriftHost(hostEntry.first);
        auto it = std::find_if(hostItems.begin(), hostItems.end(), [&](const auto& item) {
            return item.get_hostAddr() == hostAddr;
        });
        if (it != hostItems.end()) {
            std::unordered_map<std::string, std::vector<PartitionID>> leaderParts;
            for (auto& leaderEntry : hostEntry.second) {
                auto spaceId = leaderEntry.first;
                auto spaceIter = spaceIdName.find(spaceId);
                if (spaceIter == spaceIdName.end()) {
                    continue;
                }
                auto spaceName = spaceIter->second;
                leaderParts[spaceName] = std::move(leaderEntry.second);
            }
            it->set_leader_parts(std::move(leaderParts));
        }
    }

    return hostItems;
}


}  // namespace meta
}  // namespace nebula

