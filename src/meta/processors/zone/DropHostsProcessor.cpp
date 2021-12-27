/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/DropHostsProcessor.h"

namespace nebula {
namespace meta {

void DropHostsProcessor::process(const cpp2::DropHostsReq& req) {
  folly::SharedMutex::WriteHolder zHolder(LockUtils::zoneLock());
  folly::SharedMutex::WriteHolder mHolder(LockUtils::machineLock());
  auto hosts = req.get_hosts();
  if (std::unique(hosts.begin(), hosts.end()) != hosts.end()) {
    LOG(ERROR) << "Hosts have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  if (hosts.empty()) {
    LOG(ERROR) << "Hosts is empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  std::vector<std::string> data;
  std::vector<kvstore::KV> rewriteData;
  // Check that partition is not held on the host
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto spaceIterRet = doPrefix(spacePrefix);
  auto spaceIter = nebula::value(spaceIterRet).get();
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  while (spaceIter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(spaceIter->key());
    auto spaceKey = MetaKeyUtils::spaceKey(spaceId);
    auto ret = doGet(spaceKey);
    if (!nebula::ok(ret)) {
      code = nebula::error(ret);
      LOG(ERROR) << "Get Space " << spaceId
                 << " error: " << apache::thrift::util::enumNameSafe(code);
      break;
    }

    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    auto partIterRet = doPrefix(partPrefix);
    auto partIter = nebula::value(partIterRet).get();
    while (partIter->valid()) {
      auto partHosts = MetaKeyUtils::parsePartVal(partIter->val());
      for (auto& h : partHosts) {
        if (std::find(hosts.begin(), hosts.end(), h) != hosts.end()) {
          LOG(ERROR) << h << " is related with partition";
          code = nebula::cpp2::ErrorCode::E_CONFLICT;
          break;
        }
      }
      partIter->next();
    }
    spaceIter->next();
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Remove points related to the zone
  const auto& prefix = MetaKeyUtils::zonePrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "List zones failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto zoneKey = iter->key();
    auto hs = MetaKeyUtils::parseZoneHosts(iter->val());
    // Delete all hosts in the zone
    if (std::all_of(hs.begin(), hs.end(), [&hosts](auto& h) {
          return std::find(hosts.begin(), hosts.end(), h) != hosts.end();
        })) {
      auto zoneName = MetaKeyUtils::parseZoneName(zoneKey);
      LOG(INFO) << "Drop zone " << zoneName;
      auto result = checkRelatedSpaceAndCollect(zoneName);
      if (!nebula::ok(result)) {
        LOG(ERROR) << "Check related space failed";
        code = nebula::error(result);
        break;
      }

      const auto& rewrites = nebula::value(result);
      rewriteData.insert(rewriteData.end(), rewrites.begin(), rewrites.end());
      data.emplace_back(zoneKey);
    } else {
      // Delete some hosts in the zone
      for (auto& h : hosts) {
        auto it = std::find(hs.begin(), hs.end(), h);
        if (it != hs.end()) {
          hs.erase(it);
        }
      }

      auto zoneValue = MetaKeyUtils::zoneVal(hs);
      LOG(INFO) << "Zone Value: " << zoneValue;
      rewriteData.emplace_back(std::move(zoneKey), std::move(zoneValue));
    }
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      break;
    }
    iter->next();
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  // Detach the machine from cluster
  for (auto& host : hosts) {
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    auto ret = machineExist(machineKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "The host " << HostAddr(host.host, host.port) << " not existed!";
      code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
      break;
    }
    data.emplace_back(std::move(machineKey));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncMultiRemove(kDefaultSpaceId,
                             kDefaultPartId,
                             std::move(data),
                             [this, &baton](nebula::cpp2::ErrorCode result) {
                               this->handleErrorCode(result);
                               baton.post();
                             });
  baton.wait();
  doSyncPutAndUpdate(std::move(rewriteData));
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>>
DropHostsProcessor::checkRelatedSpaceAndCollect(const std::string& zoneName) {
  std::vector<kvstore::KV> data;
  const auto& prefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List spaces failed, error " << apache::thrift::util::enumNameSafe(retCode);
    return nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
  }

  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto properties = MetaKeyUtils::parseSpace(iter->val());
    size_t replicaFactor = properties.get_replica_factor();
    auto zones = properties.get_zone_names();
    LOG(INFO) << "replica_factor " << replicaFactor << " zone size " << zones.size();
    auto it = std::find(zones.begin(), zones.end(), zoneName);
    if (it != zones.end()) {
      if (zones.size() == replicaFactor) {
        LOG(ERROR) << "Zone size is same with replica factor";
        return nebula::cpp2::ErrorCode::E_CONFLICT;
      } else {
        zones.erase(it);
        properties.zone_names_ref() = zones;

        auto spaceKey = iter->key().data();
        auto spaceVal = MetaKeyUtils::spaceVal(properties);
        data.emplace_back(std::move(spaceKey), std::move(spaceVal));
      }
    }
    iter->next();
  }
  return data;
}

}  // namespace meta
}  // namespace nebula
