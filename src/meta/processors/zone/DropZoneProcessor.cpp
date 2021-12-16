/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/DropZoneProcessor.h"

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
      LOG(ERROR) << "Drop Zone Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<std::string> keys;
  keys.emplace_back(MetaKeyUtils::indexZoneKey(zoneName));
  keys.emplace_back(MetaKeyUtils::zoneKey(zoneName));
  LOG(INFO) << "Drop Zone " << zoneName;
  doSyncMultiRemoveAndUpdate(std::move(keys));
}

}  // namespace meta
}  // namespace nebula
