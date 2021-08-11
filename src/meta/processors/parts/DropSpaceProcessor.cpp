/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/DropSpaceProcessor.h"

namespace nebula {
namespace meta {

void DropSpaceProcessor::process(const cpp2::DropSpaceReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    const auto& spaceName = req.get_space_name();
    auto spaceRet = getSpaceId(spaceName);

    if (!nebula::ok(spaceRet)) {
        auto retCode = nebula::error(spaceRet);
        if (retCode == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
            if (req.get_if_exists()) {
                retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
            } else {
                LOG(ERROR) << "Drop space Failed, space " << spaceName << " not existed.";
            }
        } else {
            LOG(ERROR) << "Drop space Failed, space " << spaceName
                       << " error: " << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto spaceId = nebula::value(spaceRet);
    std::vector<std::string> deleteKeys;

    // 1. Delete related part meta data.
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Drop space Failed, space " << spaceName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
        deleteKeys.emplace_back(iter->key());
        iter->next();
    }

    // 2. Delete this space data
    deleteKeys.emplace_back(MetaServiceUtils::indexSpaceKey(spaceName));
    deleteKeys.emplace_back(MetaServiceUtils::spaceKey(spaceId));

    // 3. Delete related role data.
    auto rolePrefix = MetaServiceUtils::roleSpacePrefix(spaceId);
    auto roleRet = doPrefix(rolePrefix);
    if (!nebula::ok(roleRet)) {
        auto retCode = nebula::error(roleRet);
        LOG(ERROR) << "Drop space Failed, space " << spaceName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto roleIter = nebula::value(roleRet).get();
    while (roleIter->valid()) {
        VLOG(3) << "Revoke role "
                << MetaServiceUtils::parseRoleStr(roleIter->val())
                << " for user "
                << MetaServiceUtils::parseRoleUser(roleIter->key());
        deleteKeys.emplace_back(roleIter->key());
        roleIter->next();
    }

    // 4. Delete listener meta data
    auto lstPrefix = MetaServiceUtils::listenerPrefix(spaceId);
    auto lstRet = doPrefix(rolePrefix);
    if (!nebula::ok(lstRet)) {
        auto retCode = nebula::error(lstRet);
        LOG(ERROR) << "Drop space Failed, space " << spaceName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto lstIter = nebula::value(lstRet).get();
    while (lstIter->valid()) {
        deleteKeys.emplace_back(lstIter->key());
        lstIter->next();
    }

    // 5. Delete related statis data
    auto statiskey = MetaServiceUtils::statisKey(spaceId);
    deleteKeys.emplace_back(statiskey);

    // 6. Delte related fulltext index meta data
    auto ftPrefix = MetaServiceUtils::fulltextIndexPrefix();
    auto ftRet = doPrefix(ftPrefix);
    if (!nebula::ok(ftRet)) {
        auto retCode = nebula::error(ftRet);
        LOG(ERROR) << "Drop space Failed, space " << spaceName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto ftIter = nebula::value(ftRet).get();
    while (ftIter->valid()) {
        auto index = MetaServiceUtils::parsefulltextIndex(ftIter->val());
        if (index.get_space_id() == spaceId) {
            deleteKeys.emplace_back(ftIter->key());
        }
        ftIter->next();
    }

    doSyncMultiRemoveAndUpdate(std::move(deleteKeys));
    LOG(INFO) << "Drop space " << spaceName << ", id " << spaceId;
}

}  // namespace meta
}  // namespace nebula
