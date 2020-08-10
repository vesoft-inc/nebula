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
    auto hosts = ActiveHostsMan::getActiveHosts(kv_);
    for (const auto& host : hosts) {
        for (auto& space : spaces) {
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
    // The drop checkpoint will be skip if original host has been lost.
    auto activeHosts = ActiveHostsMan::getActiveHosts(kv_);
    for (auto& host : hosts) {
        if (std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end()) {
            std::for_each(spaces.begin(), spaces.end(), [name, host, this](auto& space) {
                auto status = client_->dropSnapshot(space, name, host).get();
                if (!status.ok()) {
                    auto msg = "failed drop checkpoint : \"%s\". on host %s. error %s";
                    auto error = folly::stringPrintf(msg,
                                                     name.c_str(),
                                                     network::NetworkUtils::toHosts({host}).c_str(),
                                                     status.toString().c_str());
                    LOG(ERROR) << error;
                }
            });
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
    auto hosts = ActiveHostsMan::getActiveHosts(kv_);
    for (const auto& host : hosts) {
        std::for_each(spaces.begin(), spaces.end(), [host, sign, this](auto& space) {
            auto status = client_->blockingWrites(space, sign, host).get();
            if (!status.ok()) {
                LOG(ERROR) << " Send blocking sign error on host : "
                           << network::NetworkUtils::toHosts({host});
            }
        });
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

