/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/AddGroupProcessor.h"

namespace nebula {
namespace meta {

void AddGroupProcessor::process(const cpp2::AddGroupReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::groupLock());
    auto groupName = req.get_group_name();
    auto zoneNames = req.get_zone_names();
    if (zoneNames.empty()) {
        LOG(ERROR) << "The zone names should not be empty.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    std::set<std::string> zoneSet(zoneNames.begin(), zoneNames.end());
    if (zoneNames.size() != zoneSet.size()) {
        LOG(ERROR) << "Conflict zone found in the group.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    // check the zone existed
    const auto& prefix = MetaServiceUtils::zonePrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Get zones failed: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto iter = nebula::value(iterRet).get();
    std::vector<std::string> zones;
    while (iter->valid()) {
        auto zoneName = MetaServiceUtils::parseZoneName(iter->key());
        zones.emplace_back(std::move(zoneName));
        iter->next();
    }

    for (auto name = zoneNames.begin(); name != zoneNames.end(); name++) {
        if (std::find(zones.begin(), zones.end(), *name) == zones.end()) {
            LOG(ERROR) << "Zone: " << *name << " not existed";
            handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
            onFinished();
            return;
        }
    }

    auto retCode = checkGroupRedundancy(zoneNames);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto groupRet = getGroupId(groupName);
    if (nebula::ok(groupRet)) {
        LOG(ERROR) << "Group " << groupName << " already existed";
        handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    } else {
        retCode = nebula::error(groupRet);
        if (retCode != nebula::cpp2::ErrorCode::E_GROUP_NOT_FOUND) {
            LOG(ERROR) << "Create Group failed, group name " << groupName << " error: "
                       << apache::thrift::util::enumNameSafe(retCode);
            handleErrorCode(retCode);
            onFinished();
            return;
        }
    }

    auto groupIdRet = autoIncrementId();
    if (!nebula::ok(groupIdRet)) {
        LOG(ERROR) << "Create Group failed";
        handleErrorCode(nebula::error(groupIdRet));
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    auto groupId = nebula::value(groupIdRet);
    data.emplace_back(MetaServiceUtils::indexGroupKey(groupName),
                      std::string(reinterpret_cast<const char*>(&groupId), sizeof(GroupID)));
    data.emplace_back(MetaServiceUtils::groupKey(groupName),
                      MetaServiceUtils::groupVal(zoneNames));

    LOG(INFO) << "Create Group: " << groupName;
    doSyncPutAndUpdate(std::move(data));
}

nebula::cpp2::ErrorCode
AddGroupProcessor::checkGroupRedundancy(std::vector<std::string> zones) {
    const auto& prefix = MetaServiceUtils::groupPrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Get groups failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    auto iter = nebula::value(iterRet).get();

    std::sort(zones.begin(), zones.end());
    while (iter->valid()) {
        auto groupName = MetaServiceUtils::parseGroupName(iter->key());
        auto zoneNames = MetaServiceUtils::parseZoneNames(iter->val());
        std::sort(zoneNames.begin(), zoneNames.end());
        if (zones == zoneNames) {
            LOG(ERROR) << "Group " << groupName
                       << " have created, although the zones order maybe not the same";
            return nebula::cpp2::ErrorCode::E_EXISTED;
        }
        iter->next();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

