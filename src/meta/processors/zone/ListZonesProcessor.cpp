/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/ListZonesProcessor.h"

namespace nebula {
namespace meta {

void ListZonesProcessor::process(const cpp2::ListZonesReq&) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& prefix = MetaKeyUtils::zonePrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto iter = nebula::value(iterRet).get();

  std::vector<cpp2::Zone> zones;
  while (iter->valid()) {
    auto zoneName = MetaKeyUtils::parseZoneName(iter->key());
    auto hosts = MetaKeyUtils::parseZoneHosts(iter->val());
    cpp2::Zone zone;
    zone.zone_name_ref() = std::move(zoneName);
    if (!hosts.empty()) {
      zone.nodes_ref() = std::move(hosts);
    } else {
      zone.nodes_ref() = {HostAddr("", 0)};
    }
    zones.emplace_back(std::move(zone));
    iter->next();
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.zones_ref() = std::move(zones);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
