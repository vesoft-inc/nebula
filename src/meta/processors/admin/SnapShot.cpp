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
        auto hostsBySpace = getHostsBySpace(space);
        for (const auto& host : hostsBySpace) {
            auto status = client_->createSnapshot(space, name, host).get();
            if (!status.ok()) {
                return cpp2::ErrorCode::E_RPC_FAILURE;
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode Snapshot::dropSnapshot(const std::string& name,
                                       const std::vector<HostAddr>& hosts) {
    std::vector<GraphSpaceID> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        LOG(ERROR) << "Can't access kvstore, ret = d"
                   << static_cast<int32_t>(ret);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }

    for (const auto& space : spaces) {
        auto hostsBySpace = getHostsBySpace(space);
        for (const auto& host : hostsBySpace) {
            if (std::find(hosts.begin(), hosts.end(), host) != hosts.end()) {
                auto status = client_->dropSnapshot(space, name, host).get();
                if (!status.ok()) {
                    auto msg = "failed drop checkpoint : \"%s\". on host %s. error %s";
                    auto error = folly::stringPrintf(msg,
                                                     name.c_str(),
                                                     network::NetworkUtils::toHosts({host}).c_str(),
                                                     status.toString().c_str());
                    LOG(ERROR) << error;
                }
            }
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
        auto hostsBySpace = getHostsBySpace(space);
        for (const auto& host : hostsBySpace) {
            auto status = client_->blockingWrites(space, sign, host).get();
            if (!status.ok()) {
                LOG(ERROR) << " Send blocking sign error on host : "
                           << network::NetworkUtils::toHosts({host});
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

std::set<HostAddr> Snapshot::getHostsBySpace(GraphSpaceID space) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    std::set<HostAddr> hosts;
    auto prefix = MetaServiceUtils::partPrefix(space);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        return {};
    }
    while (iter->valid()) {
        auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
        for (auto& ph : partHosts) {
            hosts.emplace(HostAddr(ph.get_ip(), ph.get_port()));
        }
        iter->next();
    }
    return hosts;
}

}  // namespace meta
}  // namespace nebula

