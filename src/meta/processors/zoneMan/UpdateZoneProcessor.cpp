/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/UpdateZoneProcessor.h"

namespace nebula {
namespace meta {

void AddHostIntoZoneProcessor::process(const cpp2::AddHostIntoZoneReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto zoneIdRet = getZoneId(zoneName);
    if (!zoneIdRet.ok()) {
        LOG(ERROR) << "Zone " << zoneName << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
    auto zoneValueRet = doGet(std::move(zoneKey));
    if (!zoneValueRet.ok()) {
        LOG(ERROR) << "Get zone " << zoneName << " failed";
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValueRet).value());
    auto host = req.get_node();
    auto iter = std::find(hosts.begin(), hosts.end(), host);
    if (iter != hosts.end()) {
        LOG(ERROR) << "Host " << host << " already exist in the zone " << zoneName;
        handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    hosts.emplace_back(host);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::zoneKey(zoneName),
                      MetaServiceUtils::zoneVal(std::move(hosts)));
    LOG(INFO) << "Add Host " << host << " Into Zone " << zoneName;
    doSyncPutAndUpdate(std::move(data));
}

void DropHostFromZoneProcessor::process(const cpp2::DropHostFromZoneReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto zoneIdRet = getZoneId(zoneName);
    if (!zoneIdRet.ok()) {
        LOG(ERROR) << "Zone " << zoneName << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
    auto zoneValueRet = doGet(std::move(zoneKey));
    if (!zoneValueRet.ok()) {
        LOG(ERROR) << "Get zone " << zoneName << " failed";
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValueRet).value());
    auto host = req.get_node();
    auto iter = std::find(hosts.begin(), hosts.end(), host);
    if (iter == hosts.end()) {
        LOG(ERROR) << "Host " << host << " not exist in the ";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    hosts.erase(iter);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::zoneKey(zoneName),
                      MetaServiceUtils::zoneVal(std::move(hosts)));
    LOG(INFO) << "Drop Host " << host << " From Zone " << zoneName;
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
