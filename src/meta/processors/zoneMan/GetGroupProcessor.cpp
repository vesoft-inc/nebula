/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/GetGroupProcessor.h"

namespace nebula {
namespace meta {

void GetGroupProcessor::process(const cpp2::GetGroupReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::groupLock());
    auto groupName = req.get_group_name();
    auto groupIdRet = getGroupId(groupName);
    if (!groupIdRet.ok()) {
        LOG(ERROR) << "Group: " << groupName << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto groupKey = MetaServiceUtils::groupKey(groupName);
    auto groupValueRet = doGet(std::move(groupKey));
    if (!groupValueRet.ok()) {
        LOG(ERROR) << "Get group " << groupName << " failed";
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValueRet).value());
    LOG(INFO) << "Get Group: " << groupName << " zone size: " << zoneNames.size();
    resp_.set_zone_names(std::move(zoneNames));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
