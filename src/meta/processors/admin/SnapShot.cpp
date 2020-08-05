/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/SnapShot.h"
#include <cstdlib>
#include "meta/processors/Common.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "network/NetworkUtils.h"

namespace nebula {
namespace meta {
cpp2::ErrorCode Snapshot::createSnapshot(const std::string& name) {
    std::vector<GraphSpaceID> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        LOG(ERROR) << "Can't access kvstore, ret = d"
                   << static_cast<int32_t>(ret);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }
    for (auto& space : spaces) {
        auto status = client_->createSnapshot(space, name).get();
        if (!status.ok()) {
            return cpp2::ErrorCode::E_RPC_FAILURE;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode Snapshot::dropSnapshot(const std::string& name,
                                       const std::vector<HostAddr> hosts) {
    // The drop checkpoint will be skip if original host has been lost.
    auto activeHosts = ActiveHostsMan::getActiveHosts(kv_);
    std::vector<HostAddr> realHosts;
    for (auto& host : hosts) {
        if (std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end()) {
            realHosts.emplace_back(host);
        }
    }

    std::vector<GraphSpaceID> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        LOG(ERROR) << "Can't access kvstore, ret = d"
                   << static_cast<int32_t>(ret);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }
    for (auto& space : spaces) {
        auto status = client_->dropSnapshot(space, name, realHosts).get();

        if (!status.ok()) {
            return cpp2::ErrorCode::E_RPC_FAILURE;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

bool Snapshot::getAllSpaces(std::vector<GraphSpaceID>& spaces, kvstore::ResultCode& retCode) {
    // Get all spaces
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        retCode = ret;
        return false;
    }
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        spaces.push_back(spaceId);
        iter->next();
    }
    return true;
}

cpp2::ErrorCode Snapshot::blockingWrites(storage::cpp2::EngineSignType sign) {
    std::vector<GraphSpaceID> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        LOG(ERROR) << "Can't access kvstore, ret = d"
                   << static_cast<int32_t>(ret);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }
    for (auto& space : spaces) {
        auto status = client_->blockingWrites(space, sign).get();

        if (!status.ok()) {
            return cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

std::unordered_map<HostAddr, std::vector<PartitionID>>
Snapshot::getLeaderParts(HostLeaderMap *hostLeaderMap, GraphSpaceID spaceId) {
    std::unordered_map<PartitionID, std::vector<HostAddr>> peersMap;
    std::unordered_map<HostAddr, std::vector<PartitionID>> leaderHostParts;

    auto key = MetaServiceUtils::spaceKey(spaceId);
    std::string value;
    auto code = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return leaderHostParts;
    }
    auto properties = MetaServiceUtils::parseSpace(value);

    auto leaderParts = 0;
    {
        // store peers of all paritions in peerMap
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto prefix = MetaServiceUtils::partPrefix(spaceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
            return leaderHostParts;
        }
        // Check the current cluster status, If the number of replica is
        // less than half of the total, return null list.
        while (iter->valid()) {
            auto k = iter->key();
            PartitionID partId;
            memcpy(&partId, k.data() + prefix.size(), sizeof(PartitionID));
            auto thriftPeers = MetaServiceUtils::parsePartVal(iter->val());
            std::vector<HostAddr> peers;
            peers.resize(thriftPeers.size());
            if (peers.size() < static_cast<size_t>(properties.replica_factor)/2) {
                LOG(ERROR) << "Replica part must be greater than or equal to half "
                              "the total of parts. Space : " << spaceId << " Part : " << partId;
                return leaderHostParts;
            }
            ++leaderParts;
            iter->next();
        }
    }

    if (leaderParts < properties.partition_num) {
        LOG(ERROR) << "Leader part lost, expected part : " << properties.partition_num
                   << "actual : " << leaderParts;
        return leaderHostParts;
    }

    for (const auto& host : *hostLeaderMap) {
        leaderHostParts[host.first] = std::move((*hostLeaderMap)[host.first][spaceId]);
    }

    return leaderHostParts;
}

}  // namespace meta
}  // namespace nebula

