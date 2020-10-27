/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/DropZoneProcessor.h"

namespace nebula {
namespace meta {

void DropZoneProcessor::process(const cpp2::DropZoneReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto zoneIdRet = getZoneId(zoneName);
    if (!zoneIdRet.ok()) {
        LOG(ERROR) << "Zone " << zoneName << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    // If zone belong to any group, it should not be droped.
    if (!checkGroupDependency(zoneName)) {
        return;
    }

    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexZoneKey(zoneName));
    keys.emplace_back(MetaServiceUtils::zoneKey(zoneName));
    LOG(INFO) << "Drop Zone " << zoneName;
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

bool DropZoneProcessor::checkGroupDependency(const std::string& zoneName) {
    auto prefix = MetaServiceUtils::groupPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List zones failed";
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return false;
    }

    while (iter->valid()) {
        auto zoneNames = MetaServiceUtils::parseZoneNames(iter->val());
        auto zoneIter = std::find(zoneNames.begin(), zoneNames.end(), zoneName);
        if (zoneIter != zoneNames.end()) {
            auto groupName = MetaServiceUtils::parseGroupName(iter->key());
            LOG(ERROR) << "Zone " << zoneName << " is belong to Group " << groupName;
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
