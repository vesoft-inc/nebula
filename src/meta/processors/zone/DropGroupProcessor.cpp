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
    if (!nebula::ok(groupIdRet)) {
        auto retCode = nebula::error(groupIdRet);
        if (retCode == nebula::cpp2::ErrorCode::E_GROUP_NOT_FOUND) {
            LOG(ERROR) << "Drop Group Failed, Group " << groupName << " not found.";
        } else {
            LOG(ERROR) << "Drop Group Failed, error: "
                       << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    // If any space rely on this group, it should not be droped.
    auto retCode = checkSpaceDependency(groupName);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexGroupKey(groupName));
    keys.emplace_back(MetaServiceUtils::groupKey(groupName));
    LOG(INFO) << "Drop Group: " << groupName;
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

nebula::cpp2::ErrorCode
DropGroupProcessor::checkSpaceDependency(const std::string& groupName) {
    const auto& prefix = MetaServiceUtils::spacePrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List spaces failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    auto iter = nebula::value(iterRet).get();

    while (iter->valid()) {
        auto properties = MetaServiceUtils::parseSpace(iter->val());
        if (properties.group_name_ref().has_value() &&
            *properties.group_name_ref() == groupName) {
            LOG(ERROR) << "Space " << properties.get_space_name()
                       << " is bind to the group " << groupName;
            return nebula::cpp2::ErrorCode::E_NOT_DROP;
        }
        iter->next();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
