/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/RestoreProcessor.h"

#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode RestoreProcessor::replaceHostInPartition(const HostAddr& ipv4From,
                                                                 const HostAddr& ipv4To) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  nebula::cpp2::ErrorCode retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  auto iterRet = doPrefix(spacePrefix, true);
  if (!nebula::ok(iterRet)) {
    retCode = nebula::error(iterRet);
    LOG(INFO) << "Space prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::vector<GraphSpaceID> allSpaceId;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(iter->key());
    allSpaceId.emplace_back(spaceId);
    iter->next();
  }

  std::vector<nebula::kvstore::KV> data;
  for (const auto& spaceId : allSpaceId) {
    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    auto iterPartRet = doPrefix(partPrefix, true);
    if (!nebula::ok(iterPartRet)) {
      retCode = nebula::error(iterPartRet);
      LOG(INFO) << "Part prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    iter = nebula::value(iterPartRet).get();
    while (iter->valid()) {
      bool needUpdate = false;
      auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
      for (auto& host : partHosts) {
        if (host == ipv4From) {
          needUpdate = true;
          host = ipv4To;
        }
      }
      if (needUpdate) {
        data.emplace_back(iter->key(), MetaKeyUtils::partVal(partHosts));
      }
      iter->next();
    }
  }

  retCode = kvstore_->multiPutWithoutReplicator(kDefaultSpaceId, std::move(data));
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "multiPutWithoutReplicator partition info failed, error: "
              << apache::thrift::util::enumNameSafe(retCode);
  }
  return retCode;
}

nebula::cpp2::ErrorCode RestoreProcessor::replaceHostInZone(const HostAddr& ipv4From,
                                                            const HostAddr& ipv4To) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  nebula::cpp2::ErrorCode retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
  const auto& zonePrefix = MetaKeyUtils::zonePrefix();
  auto iterRet = doPrefix(zonePrefix, true);
  if (!nebula::ok(iterRet)) {
    retCode = nebula::error(iterRet);
    LOG(INFO) << "Zone prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::vector<nebula::kvstore::KV> data;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    bool needUpdate = false;
    auto zoneName = MetaKeyUtils::parseZoneName(iter->key());
    auto hosts = MetaKeyUtils::parseZoneHosts(iter->val());
    for (auto& host : hosts) {
      if (host == ipv4From) {
        needUpdate = true;
        host = ipv4To;
      }
    }
    if (needUpdate) {
      data.emplace_back(iter->key(), MetaKeyUtils::zoneVal(hosts));
    }
    iter->next();
  }

  retCode = kvstore_->multiPutWithoutReplicator(kDefaultSpaceId, std::move(data));
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "multiPutWithoutReplicator zon info failed, error: "
              << apache::thrift::util::enumNameSafe(retCode);
  }
  return retCode;
}

void RestoreProcessor::process(const cpp2::RestoreMetaReq& req) {
  auto files = req.get_files();
  if (files.empty()) {
    LOG(INFO) << "Restore must contain the sst file.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_RESTORE_FAILURE);
    onFinished();
    return;
  }

  auto ret = kvstore_->restoreFromFiles(kDefaultSpaceId, files);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Failed to restore file";
    handleErrorCode(nebula::cpp2::ErrorCode::E_RESTORE_FAILURE);
    onFinished();
    return;
  }

  auto replaceHosts = req.get_hosts();
  if (!replaceHosts.empty()) {
    for (auto h : replaceHosts) {
      if (h.get_from_host() == h.get_to_host()) {
        continue;
      }

      auto result = replaceHostInPartition(h.get_from_host(), h.get_to_host());
      if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "Replace host in partition fails when recovered";
        handleErrorCode(result);
        onFinished();
        return;
      }

      result = replaceHostInZone(h.get_from_host(), h.get_to_host());
      if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "Replace host in zone fails when recovered";
        handleErrorCode(result);
        onFinished();
        return;
      }

      // TODO(spw): need to replace the registered machine
    }
  }

  for (auto& f : files) {
    unlink(f.c_str());
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
