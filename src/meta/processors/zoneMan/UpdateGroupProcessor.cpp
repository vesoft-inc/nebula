/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/UpdateGroupProcessor.h"

namespace nebula {
namespace meta {

void AddZoneIntoGroupProcessor::process(const cpp2::AddZoneIntoGroupReq& req) {
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

    auto zoneName = req.get_zone_name();
    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValueRet).value());
    auto iter = std::find(zoneNames.begin(), zoneNames.end(), zoneName);
    if (iter != zoneNames.end()) {
        LOG(ERROR) << "Zone " << zoneName << " already exist in the group " << groupName;
        handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    zoneNames.emplace_back(zoneName);
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(groupKey), MetaServiceUtils::groupVal(zoneNames));
    LOG(INFO) << "Add Zone " << zoneName << " Into Group " << groupName;
    doSyncPutAndUpdate(std::move(data));
}

void DropZoneFromGroupProcessor::process(const cpp2::DropZoneFromGroupReq& req) {
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
    auto groupValueRet = doGet(groupKey);
    if (!groupValueRet.ok()) {
        LOG(ERROR) << "Get group " << groupName << " failed";
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    auto zoneName = req.get_zone_name();
    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValueRet).value());
    auto iter = std::find(zoneNames.begin(), zoneNames.end(), zoneName);
    if (iter == zoneNames.end()) {
        LOG(ERROR) << "Zone " << zoneName << " not exist in the group " << groupName;
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    zoneNames.erase(iter);
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(groupKey), MetaServiceUtils::groupVal(zoneNames));
    LOG(INFO) << "Drop Zone " << zoneName << " From Group " << groupName;
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
