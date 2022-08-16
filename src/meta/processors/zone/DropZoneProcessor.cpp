/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/DropZoneProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void DropZoneProcessor::process(const cpp2::DropZoneReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto zoneName = req.get_zone_name();
  auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
  auto zoneValueRet = doGet(std::move(zoneKey));
  if (!nebula::ok(zoneValueRet)) {
    auto code = nebula::error(zoneValueRet);
    LOG(INFO) << "Drop Zone Failed, error: " << apache::thrift::util::enumNameSafe(code);
    handleErrorCode(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND);
    onFinished();
    return;
  }

  nebula::cpp2::ErrorCode code = checkSpaceReplicaZone();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  batchHolder->remove(MetaKeyUtils::zoneKey(zoneName));
  LOG(INFO) << "Drop Zone " << zoneName;

  // Drop zone associated hosts
  auto hosts = MetaKeyUtils::parseZoneHosts(std::move(nebula::value(zoneValueRet)));

  // Check Host contain partition
  for (auto& host : hosts) {
    code = checkHostPartition(host);
    CHECK_CODE_AND_BREAK();
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Drop Hosts
  for (auto& host : hosts) {
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    auto ret = machineExist(machineKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "The host " << host << " not existed!";
      code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
      break;
    }
    batchHolder->remove(std::move(machineKey));
  }

  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

nebula::cpp2::ErrorCode DropZoneProcessor::checkSpaceReplicaZone() {
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(spacePrefix);
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (!nebula::ok(ret)) {
    code = nebula::error(ret);
    LOG(INFO) << "List spaces failed, error " << apache::thrift::util::enumNameSafe(code);
    return code;
  }

  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto spaceKey = iter->key();
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    auto spaceId = MetaKeyUtils::spaceId(spaceKey);
    auto spaceZones = properties.get_zone_names();
    size_t replicaFactor = properties.get_replica_factor();
    if (replicaFactor == spaceZones.size()) {
      LOG(INFO) << "Space " << spaceId << " replica factor and zone size are the same";
      code = nebula::cpp2::ErrorCode::E_ZONE_NOT_ENOUGH;
      break;
    }
    iter->next();
  }
  return code;
}

nebula::cpp2::ErrorCode DropZoneProcessor::checkHostPartition(const HostAddr& address) {
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto spaceIterRet = doPrefix(spacePrefix);
  if (!nebula::ok(spaceIterRet)) {
    auto result = nebula::error(spaceIterRet);
    LOG(INFO) << "Get Spaces Failed, error " << apache::thrift::util::enumNameSafe(result);
    return result;
  }

  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  auto spaceIter = nebula::value(spaceIterRet).get();
  while (spaceIter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(spaceIter->key());
    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    auto partIterRet = doPrefix(partPrefix);
    if (!nebula::ok(partIterRet)) {
      code = nebula::error(partIterRet);
      LOG(INFO) << "List part failed in list hosts,  error: "
                << apache::thrift::util::enumNameSafe(code);
      return code;
    }
    auto& partIter = nebula::value(partIterRet);
    while (partIter->valid()) {
      auto hosts = MetaKeyUtils::parsePartVal(partIter->val());
      for (auto& host : hosts) {
        if (host == address) {
          LOG(INFO) << "Host " << address << " have partition on it";
          code = nebula::cpp2::ErrorCode::E_RELATED_SPACE_EXISTS;
          break;
        }
      }

      CHECK_CODE_AND_BREAK();
      partIter->next();
    }

    CHECK_CODE_AND_BREAK();
    spaceIter->next();
  }
  return code;
}

}  // namespace meta
}  // namespace nebula
