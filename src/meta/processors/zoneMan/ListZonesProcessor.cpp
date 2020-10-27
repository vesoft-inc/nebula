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
    auto prefix = MetaServiceUtils::zonePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }

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

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_zones(std::move(zones));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
