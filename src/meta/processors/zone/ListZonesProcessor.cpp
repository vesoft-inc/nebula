/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/ListZonesProcessor.h"

namespace nebula {
namespace meta {

void ListZonesProcessor::process(const cpp2::ListZonesReq&) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
    const auto& prefix = MetaServiceUtils::zonePrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto iter = nebula::value(iterRet).get();

    std::vector<cpp2::Zone> zones;
    while (iter->valid()) {
        auto zoneName = MetaServiceUtils::parseZoneName(iter->key());
        auto hosts = MetaServiceUtils::parseZoneHosts(iter->val());
        cpp2::Zone zone;
        zone.set_zone_name(std::move(zoneName));
        zone.set_nodes(std::move(hosts));
        zones.emplace_back(std::move(zone));
        iter->next();
    }

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_zones(std::move(zones));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
