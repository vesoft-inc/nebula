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

cpp2::ErrorCode Snapshot::sendBlockSign(storage::cpp2::EngineSignType sign) {
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
        auto status = client_->sendBlockSign(space, sign).get();

        if (!status.ok()) {
            return cpp2::ErrorCode::E_RPC_FAILURE;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}
}  // namespace meta
}  // namespace nebula

