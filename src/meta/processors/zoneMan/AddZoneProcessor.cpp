/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/AddZoneProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void AddZoneProcessor::process(const cpp2::AddZoneReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto nodes = req.get_nodes();
    if (nodes.empty()) {
        LOG(ERROR) << "The hosts should not be empty";
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    std::set<HostAddr> nodeSet(nodes.begin(), nodes.end());
    if (nodes.size() != nodeSet.size()) {
        LOG(ERROR) << "Conflict host found in the zone";
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
    if (!nebula::ok(activeHostsRet)) {
        auto retCode = nebula::error(activeHostsRet);
        LOG(ERROR) << "Create zone failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto activeHosts = nebula::value(activeHostsRet);

    std::sort(activeHosts.begin(), activeHosts.end());
    if (!std::includes(activeHosts.begin(), activeHosts.end(), nodeSet.begin(), nodeSet.end())) {
        LOG(ERROR) << "Host not exist";
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    auto zoneIdRet = getZoneId(zoneName);
    if (nebula::ok(zoneIdRet)) {
        LOG(ERROR) << "Zone " << zoneName  << " already existed";
        handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    } else {
        auto retCode = nebula::error(zoneIdRet);
        if (retCode != nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND) {
            LOG(ERROR) << "Create zone failed, zone name " << zoneName << " error: "
                       << apache::thrift::util::enumNameSafe(retCode);
            handleErrorCode(retCode);
            onFinished();
            return;
        }
    }

    // Check the node is not include in another zone
    auto retCode = checkHostNotOverlap(nodes);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto zoneRet = autoIncrementId();
    if (!nebula::ok(zoneRet)) {
        LOG(ERROR) << "Create Zone failed";
        handleErrorCode(nebula::error(zoneRet));
        onFinished();
        return;
    }

    auto zoneId = nebula::value(zoneRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexZoneKey(zoneName),
                      std::string(reinterpret_cast<const char*>(&zoneId), sizeof(ZoneID)));
    data.emplace_back(MetaServiceUtils::zoneKey(zoneName),
                      MetaServiceUtils::zoneVal(nodes));

    LOG(INFO) << "Create Zone: " << zoneName;
    doSyncPutAndUpdate(std::move(data));
}

nebula::cpp2::ErrorCode
AddZoneProcessor::checkHostNotOverlap(const std::vector<HostAddr>& nodes) {
    const auto& prefix = MetaServiceUtils::zonePrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Get zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
        auto zoneName = MetaServiceUtils::parseZoneName(iter->key());
        auto hosts = MetaServiceUtils::parseZoneHosts(iter->val());
        for (auto& node : nodes) {
            auto hostIter = std::find(hosts.begin(), hosts.end(), node);
            if (hostIter != hosts.end()) {
                LOG(ERROR) << "Host overlap found in zone " << zoneName;
                return nebula::cpp2::ErrorCode::E_INVALID_PARM;
            }
        }
        iter->next();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

