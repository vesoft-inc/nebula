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
    folly::Promise<Status> promise;
    auto future = promise.getFuture();

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

cpp2::ErrorCode Snapshot::dropSnapshot(const std::string& name) {
    folly::Promise<Status> promise;
    auto future = promise.getFuture();

    std::vector<GraphSpaceID> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        LOG(ERROR) << "Can't access kvstore, ret = d"
                   << static_cast<int32_t>(ret);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }
    for (auto& space : spaces) {
        auto status = client_->dropSnapshot(space, name).get();

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
    folly::Promise<Status> promise;
    auto future = promise.getFuture();

    std::vector<GraphSpaceID> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        LOG(ERROR) << "Can't access kvstore, ret = d"
                   << static_cast<int32_t>(ret);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }
    std::unique_ptr<HostLeaderMap> hostLeaderMap = std::make_unique<HostLeaderMap>();
    auto status = client_->getLeaderDist(hostLeaderMap.get()).get();

    if (!status.ok() || hostLeaderMap->empty()) {
        return cpp2::ErrorCode::E_RPC_FAILURE;
    }

    std::vector<folly::SemiFuture<Status>> futures;
    for (const auto& space : spaces) {
        const auto& hostParts = buildLeaderPlan(hostLeaderMap.get(), space);
        for (const auto& hostPart : hostParts) {
            auto host = hostPart.first;
            auto parts = hostPart.second;
            for (auto& part : parts) {
                futures.emplace_back(client_->blockingWrites(space, part, host, sign));
            }
        }
    }

    int32_t failed = 0;
    folly::collectAll(futures).then([&](const std::vector<folly::Try<Status>>& tries) {
        for (const auto& t : tries) {
            if (!t.value().ok()) {
                ++failed;
            }
        }
    }).wait();
    LOG(INFO) << failed << " partiton failed to block leader";
    if (failed > 0) {
        return cpp2::ErrorCode::E_RPC_FAILURE;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

std::unordered_map<HostAddr, std::vector<PartitionID>>
Snapshot::buildLeaderPlan(HostLeaderMap* hostLeaderMap, GraphSpaceID spaceId) {
    std::unordered_map<PartitionID, std::vector<HostAddr>> peersMap;
    std::unordered_map<HostAddr, std::vector<PartitionID>> leaderHostParts;
    size_t leaderParts = 0;
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
        while (iter->valid()) {
            auto key = iter->key();
            PartitionID partId;
            memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
            auto thriftPeers = MetaServiceUtils::parsePartVal(iter->val());
            std::vector<HostAddr> peers;
            peers.resize(thriftPeers.size());
            std::transform(thriftPeers.begin(), thriftPeers.end(), peers.begin(),
                           [] (const auto& h) { return HostAddr(h.get_ip(), h.get_port()); });
            peersMap[partId] = std::move(peers);
            ++leaderParts;
            iter->next();
        }
    }

    std::unordered_set<HostAddr> activeHosts;
    for (const auto& host : *hostLeaderMap) {
        activeHosts.emplace(host.first);
        leaderHostParts[host.first] = std::move((*hostLeaderMap)[host.first][spaceId]);
    }

    return leaderHostParts;
}
}  // namespace meta
}  // namespace nebula

