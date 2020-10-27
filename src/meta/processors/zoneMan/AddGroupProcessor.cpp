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
        handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    std::set<std::string> zoneSet(zoneNames.begin(), zoneNames.end());
    if (zoneNames.size() != zoneSet.size()) {
        LOG(ERROR) << "Conflict zone found in the group.";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    if (!checkGroupRedundancy(zoneNames)) {
        handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    auto groupRet = getGroupId(groupName);
    if (groupRet.ok()) {
        LOG(ERROR) << "Group " << groupName << " already existed";
        handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
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

bool AddGroupProcessor::checkGroupRedundancy(std::vector<std::string> zones) {
    auto prefix = MetaServiceUtils::groupPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get groups failed";
        return false;
    }

    std::sort(zones.begin(), zones.end());
    while (iter->valid()) {
        auto groupName = MetaServiceUtils::parseGroupName(iter->key());
        auto zoneNames = MetaServiceUtils::parseZoneNames(iter->val());
        std::sort(zoneNames.begin(), zoneNames.end());
        if (zones == zoneNames) {
            LOG(ERROR) << "Group " << groupName
                       << " have created, although the zones order maybe not the same";
            return false;
        }
        iter->next();
    }
    return true;
}

}  // namespace meta
}  // namespace nebula

