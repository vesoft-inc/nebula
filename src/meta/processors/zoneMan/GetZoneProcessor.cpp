/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/GetZoneProcessor.h"

namespace nebula {
namespace meta {

void GetZoneProcessor::process(const cpp2::GetZoneReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto zoneIdRet = getZoneId(zoneName);
    if (!nebula::ok(zoneIdRet)) {
        auto retCode = nebula::error(zoneIdRet);
        if (retCode == nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND) {
            LOG(ERROR) << "Get Zone Failed, Zone " << zoneName << " not found.";
        } else {
            LOG(ERROR) << "Get Zone Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
    auto zoneValueRet = doGet(std::move(zoneKey));
    if (!nebula::ok(zoneValueRet)) {
        auto retCode = nebula::error(zoneValueRet);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            retCode = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
        }
        LOG(ERROR) << "Get zone " << zoneName << " failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto hosts = MetaServiceUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
    LOG(INFO) << "Get Zone: " << zoneName << " node size: " << hosts.size();
    resp_.set_hosts(std::move(hosts));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
