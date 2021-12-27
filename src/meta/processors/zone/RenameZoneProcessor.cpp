/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/RenameZoneProcessor.h"

namespace nebula {
namespace meta {

void RenameZoneProcessor::process(const cpp2::RenameZoneReq& req) {
  folly::SharedMutex::WriteHolder zHolder(LockUtils::zoneLock());

  auto originalZoneName = req.get_original_zone_name();
  auto originalZoneKey = MetaKeyUtils::zoneKey(originalZoneName);
  auto originalZoneValueRet = doGet(std::move(originalZoneKey));
  if (!nebula::ok(originalZoneValueRet)) {
    LOG(ERROR) << "Zone " << originalZoneName << " not existed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
    onFinished();
    return;
  }

  auto originalZoneValue = nebula::value(originalZoneValueRet);
  auto zoneName = req.get_zone_name();
  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (nebula::ok(zoneValueRet)) {
    LOG(ERROR) << "Zone " << zoneName << " have existed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
    onFinished();
    return;
  }

  const auto& prefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    LOG(ERROR) << "List spaces failed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto spaceKey = iter->key();
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    auto zones = properties.get_zone_names();
    auto it = std::find(zones.begin(), zones.end(), originalZoneName);
    if (it != zones.end()) {
      std::replace(zones.begin(), zones.end(), originalZoneName, zoneName);
      properties.zone_names_ref() = zones;
      auto spaceVal = MetaKeyUtils::spaceVal(properties);
      data.emplace_back(spaceKey, std::move(spaceVal));
    }
    iter->next();
  }

  folly::Baton<true, std::atomic> baton;
  auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
  std::vector<std::string> keys = {originalZoneKey};
  kvstore_->asyncMultiRemove(kDefaultSpaceId,
                             kDefaultPartId,
                             std::move(keys),
                             [&result, &baton](nebula::cpp2::ErrorCode code) {
                               if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                                 result = code;
                                 LOG(INFO) << "Remove data error on meta server";
                               }
                               baton.post();
                             });
  baton.wait();
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    this->handleErrorCode(result);
    this->onFinished();
    return;
  }

  data.emplace_back(zoneKey, originalZoneValue);
  doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
