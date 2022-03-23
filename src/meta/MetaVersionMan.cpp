/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/MetaVersionMan.h"

#include "common/fs/FileUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/upgrade/MetaDataUpgrade.h"
#include "meta/upgrade/v2/MetaServiceUtilsV2.h"

DEFINE_bool(null_type, true, "set schema to support null type");
DEFINE_bool(print_info, false, "enable to print the rewrite data");
DEFINE_uint32(string_index_limit, 64, "string index key length limit");

namespace nebula {
namespace meta {

static const std::string kMetaVersionKey = "__meta_version__";  // NOLINT

// static
MetaVersion MetaVersionMan::getMetaVersionFromKV(kvstore::KVStore* kv) {
  CHECK_NOTNULL(kv);
  std::string value;
  auto code = kv->get(kDefaultSpaceId, kDefaultPartId, kMetaVersionKey, &value, true);
  if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
    auto version = *reinterpret_cast<const MetaVersion*>(value.data());
    return version;
  } else {
    return getVersionByHost(kv);
  }
}

// static
MetaVersion MetaVersionMan::getVersionByHost(kvstore::KVStore* kv) {
  const auto& hostPrefix = nebula::MetaKeyUtils::hostPrefix();
  std::unique_ptr<nebula::kvstore::KVIterator> iter;
  auto code = kv->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter, true);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return MetaVersion::UNKNOWN;
  }
  if (iter->valid()) {
    auto v1KeySize = hostPrefix.size() + sizeof(int64_t);
    return (iter->key().size() == v1KeySize) ? MetaVersion::V1 : MetaVersion::V3;
  }
  // No hosts exists, regard as version 3
  return MetaVersion::V3;
}

// static
bool MetaVersionMan::setMetaVersionToKV(kvstore::KVEngine* engine, MetaVersion version) {
  CHECK_NOTNULL(engine);
  std::string versionValue =
      std::string(reinterpret_cast<const char*>(&version), sizeof(MetaVersion));
  auto code = engine->put(kMetaVersionKey, std::move(versionValue));
  return code == nebula::cpp2::ErrorCode::SUCCEEDED;
}

Status MetaVersionMan::updateMetaV2ToV3(kvstore::KVEngine* engine) {
  CHECK_NOTNULL(engine);
  auto snapshot = folly::sformat("META_UPGRADE_SNAPSHOT_{}", MetaKeyUtils::genTimestampStr());

  std::string path = folly::sformat("{}/checkpoints/{}", engine->getDataRoot(), snapshot);
  if (!fs::FileUtils::exist(path) && !fs::FileUtils::makeDir(path)) {
    LOG(INFO) << "Make checkpoint dir: " << path << " failed";
    return Status::Error("Create snapshot file failed");
  }

  std::string dataPath = folly::sformat("{}/data", path);
  auto code = engine->createCheckpoint(dataPath);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Create snapshot failed: " << snapshot;
    return Status::Error("Create snapshot failed");
  }

  auto status = doUpgradeV2ToV3(engine);
  if (!status.ok()) {
    // rollback by snapshot
    return status;
  }

  // delete snapshot file
  auto checkpointPath = folly::sformat("{}/checkpoints/{}", engine->getDataRoot(), snapshot);
  if (fs::FileUtils::exist(checkpointPath) && !fs::FileUtils::remove(checkpointPath.data(), true)) {
    LOG(INFO) << "Delete snapshot: " << snapshot << " failed, You need to delete it manually";
  }
  return Status::OK();
}

Status MetaVersionMan::doUpgradeV2ToV3(kvstore::KVEngine* engine) {
  MetaDataUpgrade upgrader(engine);
  // Step 1: Upgrade HeartBeat into machine list
  {
    // collect all hosts association with zone
    std::vector<HostAddr> zoneHosts;
    const auto& zonePrefix = MetaKeyUtils::zonePrefix();
    std::unique_ptr<kvstore::KVIterator> zoneIter;
    auto code = engine->prefix(zonePrefix, &zoneIter);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Get active hosts failed";
      return Status::Error("Get hosts failed");
    }

    while (zoneIter->valid()) {
      auto hosts = MetaKeyUtils::parseZoneHosts(zoneIter->val());
      if (!hosts.empty()) {
        zoneHosts.insert(zoneHosts.end(), hosts.begin(), hosts.end());
      }
      zoneIter->next();
    }

    const auto& prefix = MetaKeyUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    code = engine->prefix(prefix, &iter);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Get active hosts failed";
      return Status::Error("Get hosts failed");
    }

    std::vector<kvstore::KV> data;
    while (iter->valid()) {
      auto info = HostInfo::decode(iter->val());

      if (info.role_ == meta::cpp2::HostRole::STORAGE) {
        // Save the machine information
        auto host = MetaKeyUtils::parseHostKey(iter->key());
        auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
        data.emplace_back(std::move(machineKey), "");

        auto hostIt = std::find(zoneHosts.begin(), zoneHosts.end(), host);
        if (hostIt == zoneHosts.end()) {
          // Save the zone information
          auto zoneName = folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
          auto zoneKey = MetaKeyUtils::zoneKey(std::move(zoneName));
          auto zoneVal = MetaKeyUtils::zoneVal({host});
          data.emplace_back(std::move(zoneKey), std::move(zoneVal));
        }
      }
      iter->next();
    }
    auto status = upgrader.saveMachineAndZone(std::move(data));
    if (!status.ok()) {
      LOG(INFO) << status;
      return status;
    }
  }

  // Step 2: Update Create space properties about Group
  {
    const auto& prefix = MetaKeyUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = engine->prefix(prefix, &iter);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Get spaces failed";
      return Status::Error("Get spaces failed");
    }

    while (iter->valid()) {
      if (FLAGS_print_info) {
        upgrader.printSpacesV2(iter->val());
      }
      auto status = upgrader.rewriteSpacesV2ToV3(iter->key(), iter->val());
      if (!status.ok()) {
        LOG(INFO) << status;
        return status;
      }
      iter->next();
    }
  }
  if (!setMetaVersionToKV(engine, MetaVersion::V3)) {
    return Status::Error("Persist meta version failed");
  } else {
    return Status::OK();
  }
}

}  // namespace meta
}  // namespace nebula
