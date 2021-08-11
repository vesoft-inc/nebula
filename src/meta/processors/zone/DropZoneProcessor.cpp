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
    if (!nebula::ok(zoneIdRet)) {
        auto retCode = nebula::error(zoneIdRet);
         if (retCode == nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND) {
            LOG(ERROR) << "Drop Zone Failed, Zone " << zoneName << " not found.";
        } else {
            LOG(ERROR) << "Drop Zone Failed, error: "
                       << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    // If zone belong to any group, it should not be droped.
    auto retCode = checkGroupDependency(zoneName);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexZoneKey(zoneName));
    keys.emplace_back(MetaServiceUtils::zoneKey(zoneName));
    LOG(INFO) << "Drop Zone " << zoneName;
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

nebula::cpp2::ErrorCode
DropZoneProcessor::checkGroupDependency(const std::string& zoneName) {
    const auto& prefix = MetaServiceUtils::groupPrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List zones failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    auto iter = nebula::value(iterRet).get();

    while (iter->valid()) {
        auto zoneNames = MetaServiceUtils::parseZoneNames(iter->val());
        auto zoneIter = std::find(zoneNames.begin(), zoneNames.end(), zoneName);
        if (zoneIter != zoneNames.end()) {
            auto groupName = MetaServiceUtils::parseGroupName(iter->key());
            LOG(ERROR) << "Zone " << zoneName << " is belong to Group " << groupName;
            return nebula::cpp2::ErrorCode::E_NOT_DROP;
        }
        iter->next();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
