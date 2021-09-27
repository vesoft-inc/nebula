/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zone/ListZonesProcessor.h"

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

  const auto& groupPrefix = MetaServiceUtils::groupPrefix();
  auto groupIterRet = doPrefix(groupPrefix);
  if (!nebula::ok(groupIterRet)) {
    auto retCode = nebula::error(groupIterRet);
    LOG(ERROR) << "Get groups failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto groupIter = nebula::value(groupIterRet).get();
  while (groupIter->valid()) {
    auto zoneNames = MetaServiceUtils::parseZoneNames(groupIter->val());
    for (auto& name : zoneNames) {
      auto it = std::find_if(zones.begin(), zones.end(), [&name](const auto& zone) {
        return name == zone.get_zone_name();
      });
      if (it == zones.end()) {
        cpp2::Zone zone;
        zone.set_zone_name(name);
        zone.set_nodes({});
        zones.emplace_back(std::move(zone));
      }
    }
    iter->next();
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_zones(std::move(zones));
  onFinished();
}

}  // namespace meta
}  // namespace nebula
