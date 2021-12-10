/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/SplitZoneProcessor.h"

namespace nebula {
namespace meta {

void SplitZoneProcessor::process(const cpp2::SplitZoneReq& req) {
  auto zoneName = req.get_zone_name();
  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (!nebula::ok(zoneValueRet)) {
    LOG(ERROR) << "Zone " << zoneName << " not existed error: "
               << apache::thrift::util::enumNameSafe(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
    handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
    onFinished();
    return;
  }

  auto oneZoneName = req.get_one_zone_name();
  auto anotherZoneName = req.get_another_zone_name();
  auto oneZoneHosts = req.get_one_zone_hosts();
  // Confirm that there are no duplicates in the parameters.
  if (std::unique(oneZoneHosts.begin(), oneZoneHosts.end()) != oneZoneHosts.end()) {
    LOG(ERROR) << "Zones have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  auto anotherZoneHosts = req.get_another_zone_hosts();
  // Confirm that there are no duplicates in the parameters.
  if (std::unique(anotherZoneHosts.begin(), anotherZoneHosts.end()) != anotherZoneHosts.end()) {
    LOG(ERROR) << "Zones have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (oneZoneHosts.empty() || anotherZoneHosts.empty()) {
    LOG(ERROR) << "Hosts should not be empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  for (auto& host : oneZoneHosts) {
    auto iter = std::find(anotherZoneHosts.begin(), anotherZoneHosts.end(), host);
    if (iter != anotherZoneHosts.end()) {
      LOG(ERROR) << "Host " << host << " repeat with another zone hosts";
      code = nebula::cpp2::ErrorCode::E_CONFLICT;
      break;
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  std::vector<HostAddr> unionHosts;
  std::sort(oneZoneHosts.begin(), oneZoneHosts.end());
  std::sort(anotherZoneHosts.begin(), anotherZoneHosts.end());
  std::set_union(oneZoneHosts.begin(),
                 oneZoneHosts.end(),
                 anotherZoneHosts.begin(),
                 anotherZoneHosts.end(),
                 std::back_inserter(unionHosts));

  auto zoneHosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));
  if (unionHosts.size() != zoneHosts.size()) {
    LOG(ERROR) << "The total host is not all hosts";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  for (auto& host : unionHosts) {
    auto iter = std::find(zoneHosts.begin(), zoneHosts.end(), host);
    if (iter == zoneHosts.end()) {
      LOG(ERROR) << "Host " << host << " not exist in original zone";
      code = nebula::cpp2::ErrorCode::E_INVALID_PARM;
      break;
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Remove original zone
  folly::Baton<true, std::atomic> baton;
  std::vector<std::string> keys = {zoneKey};
  auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiRemove(kDefaultSpaceId,
                             kDefaultPartId,
                             std::move(keys),
                             [&result, &baton](nebula::cpp2::ErrorCode res) {
                               if (nebula::cpp2::ErrorCode::SUCCEEDED != res) {
                                 result = res;
                                 LOG(ERROR) << "Remove data error on meta server";
                               }
                               baton.post();
                             });
  baton.wait();
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    this->handleErrorCode(result);
    this->onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  auto oneZoneKey = MetaKeyUtils::zoneKey(std::move(oneZoneName));
  auto oneZoneVal = MetaKeyUtils::zoneVal(std::move(oneZoneHosts));
  data.emplace_back(std::move(oneZoneKey), std::move(oneZoneVal));

  auto anotherZoneKey = MetaKeyUtils::zoneKey(std::move(anotherZoneName));
  auto anotherZoneVal = MetaKeyUtils::zoneVal(std::move(anotherZoneHosts));
  data.emplace_back(std::move(anotherZoneKey), std::move(anotherZoneVal));
  doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
