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
    if (!nebula::ok(groupIdRet)) {
        auto retCode = nebula::error(groupIdRet);
        LOG(ERROR) << "Get Group failed, group " << groupName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto groupKey = MetaServiceUtils::groupKey(groupName);
    auto groupValueRet = doGet(std::move(groupKey));
    if (!nebula::ok(groupValueRet)) {
        auto retCode = nebula::error(groupValueRet);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            retCode = nebula::cpp2::ErrorCode::E_GROUP_NOT_FOUND;
        }
        LOG(ERROR) << "Get group " << groupName << " failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto zoneName = req.get_zone_name();
    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(nebula::value(groupValueRet)));
    auto iter = std::find(zoneNames.begin(), zoneNames.end(), zoneName);
    if (iter != zoneNames.end()) {
        LOG(ERROR) << "Zone " << zoneName << " already exist in the group " << groupName;
        handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    const auto& zonePrefix = MetaServiceUtils::zonePrefix();
    auto iterRet = doPrefix(zonePrefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Get zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto zoneIter = nebula::value(iterRet).get();

    bool found = false;
    while (zoneIter->valid()) {
        auto name = MetaServiceUtils::parseZoneName(zoneIter->key());
        if (name == zoneName) {
            found = true;
            break;
        }
        zoneIter->next();
    }

    if (!found) {
        LOG(ERROR) << "Zone " << zoneName << " not found";
        handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
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
    if (!nebula::ok(groupIdRet)) {
        auto retCode = nebula::error(groupIdRet);
        LOG(ERROR) << " Get Group " << groupName << " failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto groupKey = MetaServiceUtils::groupKey(groupName);
    auto groupValueRet = doGet(groupKey);
    if (!nebula::ok(groupValueRet)) {
        auto retCode = nebula::error(groupValueRet);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            retCode = nebula::cpp2::ErrorCode::E_GROUP_NOT_FOUND;
        }
        LOG(ERROR) << "Get group " << groupName << " failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto zoneName = req.get_zone_name();
    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(nebula::value(groupValueRet)));
    auto iter = std::find(zoneNames.begin(), zoneNames.end(), zoneName);
    if (iter == zoneNames.end()) {
        LOG(ERROR) << "Zone " << zoneName << " not exist in the group " << groupName;
        handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
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
