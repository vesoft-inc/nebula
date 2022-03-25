/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/GetZoneProcessor.h"

namespace nebula {
namespace meta {

void GetZoneProcessor::process(const cpp2::GetZoneReq& req) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  auto zoneName = req.get_zone_name();
  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (!nebula::ok(zoneValueRet)) {
    auto retCode = nebula::error(zoneValueRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
    }
    LOG(INFO) << "Get zone " << zoneName
              << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
  LOG(INFO) << "Get Zone: " << zoneName << " node size: " << hosts.size();
  resp_.hosts_ref() = std::move(hosts);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
