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
    auto& groupName = req.get_group_name();
    auto groupIdRet = getGroupId(groupName);
    if (!nebula::ok(groupIdRet)) {
        auto retCode = nebula::error(groupIdRet);
        if (retCode == nebula::cpp2::ErrorCode::E_GROUP_NOT_FOUND) {
            LOG(ERROR) << "Get Group Failed, Group " << groupName << " not found.";
        } else {
            LOG(ERROR) << "Get Group Failed, error: "
                       << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto groupKey = MetaServiceUtils::groupKey(groupName);
    auto groupValueRet = doGet(std::move(groupKey));
    if (!nebula::ok(groupValueRet)) {
        auto retCode = nebula::error(groupValueRet);
        LOG(ERROR) << "Get group " << groupName << " failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(nebula::value(groupValueRet)));
    LOG(INFO) << "Get Group: " << groupName << " zone size: " << zoneNames.size();
    resp_.set_zone_names(std::move(zoneNames));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
