/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/DropGroupProcessor.h"

namespace nebula {
namespace meta {

void DropGroupProcessor::process(const cpp2::DropGroupReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::groupLock());
    auto groupName = req.get_group_name();
    auto groupIdRet = getGroupId(groupName);
    if (!groupIdRet.ok()) {
        LOG(ERROR) << "Group: " << groupName << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    // If any space rely on this group, it should not be droped.
    if (!checkSpaceDependency(groupName)) {
        return;
    }

    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexGroupKey(groupName));
    keys.emplace_back(MetaServiceUtils::groupKey(groupName));
    LOG(INFO) << "Drop Group: " << groupName;
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

bool DropGroupProcessor::checkSpaceDependency(const std::string& groupName) {
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List spaces failed";
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return false;
    }

    while (iter->valid()) {
        auto properties = MetaServiceUtils::parseSpace(iter->val());
        if (*properties.get_group_name() == groupName) {
            LOG(ERROR) << "Space " << properties.get_space_name()
                       << " is bind to the group " << groupName;
            handleErrorCode(cpp2::ErrorCode::E_NOT_DROP);
            onFinished();
            return false;
        }
        iter->next();
    }
    return true;
}
}  // namespace meta
}  // namespace nebula
