/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/DropSpaceProcessor.h"

namespace nebula {
namespace meta {

void DropSpaceProcessor::process(const cpp2::DropSpaceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto spaceRet = getSpaceId(req.get_space_name());

    if (!spaceRet.ok()) {
        handleErrorCode(req.get_if_exists() ? cpp2::ErrorCode::SUCCEEDED :
                                             MetaCommon::to(spaceRet.status()));
        onFinished();
        return;
    }

    auto spaceId = spaceRet.value();
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    std::vector<std::string> deleteKeys;

    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }

    while (iter->valid()) {
        deleteKeys.emplace_back(iter->key());
        iter->next();
    }

    deleteKeys.emplace_back(MetaServiceUtils::indexSpaceKey(req.get_space_name()));
    deleteKeys.emplace_back(MetaServiceUtils::spaceKey(spaceId));

    // delete related role data.
    auto rolePrefix = MetaServiceUtils::roleSpacePrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> roleIter;
    auto roleRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, rolePrefix, &roleIter);
    if (roleRet != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(roleRet));
        onFinished();
        return;
    }
    while (roleIter->valid()) {
        auto user = MetaServiceUtils::parseRoleUser(roleIter->key());
        VLOG(3) << "Revoke role "
                << MetaServiceUtils::parseRoleStr(roleIter->val())
                << " for user "
                << MetaServiceUtils::parseRoleUser(roleIter->key());
        deleteKeys.emplace_back(roleIter->key());
        roleIter->next();
    }

    // delete listener meta data
    auto lstPrefix = MetaServiceUtils::listenerPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> lstIter;
    auto listenerRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, lstPrefix, &lstIter);
    if (listenerRet != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(listenerRet));
        onFinished();
        return;
    }
    while (lstIter->valid()) {
        deleteKeys.emplace_back(lstIter->key());
        lstIter->next();
    }

    // Delete statis data if it exists
    auto statiskey = MetaServiceUtils::statisKey(spaceId);
    deleteKeys.emplace_back(statiskey);

    doSyncMultiRemoveAndUpdate(std::move(deleteKeys));
    LOG(INFO) << "Drop space " << req.get_space_name() << ", id " << spaceId;
}

}  // namespace meta
}  // namespace nebula
