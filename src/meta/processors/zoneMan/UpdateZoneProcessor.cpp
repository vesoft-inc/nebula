/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/UpdateZoneProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void AddHostIntoZoneProcessor::process(const cpp2::AddHostIntoZoneReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto zoneIdRet = getZoneId(zoneName);
    if (!nebula::ok(zoneIdRet)) {
        auto retCode = nebula::error(zoneIdRet);
        LOG(ERROR) << "Get Zone failed, zone " << zoneName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
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
        LOG(ERROR) << "Get zone " << zoneName << " failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto hosts = MetaServiceUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
    auto host = req.get_node();
    // check this host not exist in all zones
    const auto& prefix = MetaServiceUtils::zonePrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Get zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
        auto name = MetaServiceUtils::parseZoneName(iter->key());
        auto zoneHosts = MetaServiceUtils::parseZoneHosts(iter->val());
        auto hostIter = std::find(zoneHosts.begin(), zoneHosts.end(), host);
        if (hostIter != zoneHosts.end()) {
            LOG(ERROR) << "Host overlap found in zone " << name;
            handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }
        iter->next();
    }

    auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
    if (!nebula::ok(activeHostsRet)) {
        auto retCode = nebula::error(activeHostsRet);
        LOG(ERROR) << "Get hosts failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto activeHosts = nebula::value(activeHostsRet);

    auto found = std::find(activeHosts.begin(), activeHosts.end(), host);
    if (found == activeHosts.end()) {
        LOG(ERROR) << "Host " << host << " not exist";
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    hosts.emplace_back(host);
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(zoneKey), MetaServiceUtils::zoneVal(std::move(hosts)));
    LOG(INFO) << "Add Host " << host << " Into Zone " << zoneName;
    doSyncPutAndUpdate(std::move(data));
}

void DropHostFromZoneProcessor::process(const cpp2::DropHostFromZoneReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto zoneIdRet = getZoneId(zoneName);
    if (!nebula::ok(zoneIdRet)) {
        auto retCode = nebula::error(zoneIdRet);
        LOG(ERROR) << "Get Zone failed, group " << zoneName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
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
    auto host = req.get_node();
    auto iter = std::find(hosts.begin(), hosts.end(), host);
    if (iter == hosts.end()) {
        LOG(ERROR) << "Host " << host << " not exist in the zone " << zoneName;
        handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
        onFinished();
        return;
    }

    hosts.erase(iter);
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(zoneKey), MetaServiceUtils::zoneVal(std::move(hosts)));
    LOG(INFO) << "Drop Host " << host << " From Zone " << zoneName;
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
