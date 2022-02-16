/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/RenameZoneProcessor.h"

#include <folly/SharedMutex.h>         // for SharedMutex
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>  // for find, replace
#include <memory>     // for unique_ptr, make_unique
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for basic_string, operator==
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "kvstore/LogEncoder.h"               // for BatchHolder, encodeBa...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void RenameZoneProcessor::process(const cpp2::RenameZoneReq& req) {
  folly::SharedMutex::WriteHolder zHolder(LockUtils::zoneLock());
  folly::SharedMutex::WriteHolder sHolder(LockUtils::spaceLock());

  auto originalZoneName = req.get_original_zone_name();
  auto originalZoneKey = MetaKeyUtils::zoneKey(originalZoneName);
  auto originalZoneValueRet = doGet(std::move(originalZoneKey));
  if (!nebula::ok(originalZoneValueRet)) {
    LOG(INFO) << "Zone " << originalZoneName << " not existed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
    onFinished();
    return;
  }

  auto originalZoneValue = nebula::value(originalZoneValueRet);
  auto zoneName = req.get_zone_name();
  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (nebula::ok(zoneValueRet)) {
    LOG(INFO) << "Zone " << zoneName << " have existed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
    onFinished();
    return;
  }

  const auto& prefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "List spaces failed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
    onFinished();
    return;
  }

  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto id = MetaKeyUtils::spaceId(iter->key());
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    auto zones = properties.get_zone_names();
    auto it = std::find(zones.begin(), zones.end(), originalZoneName);
    if (it != zones.end()) {
      std::replace(zones.begin(), zones.end(), originalZoneName, zoneName);
      properties.zone_names_ref() = zones;
      auto spaceKey = MetaKeyUtils::spaceKey(id);
      auto spaceVal = MetaKeyUtils::spaceVal(properties);
      batchHolder->put(std::move(spaceKey), std::move(spaceVal));
    }
    iter->next();
  }

  batchHolder->remove(MetaKeyUtils::zoneKey(originalZoneName));
  batchHolder->put(std::move(zoneKey), std::move(originalZoneValue));
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

}  // namespace meta
}  // namespace nebula
