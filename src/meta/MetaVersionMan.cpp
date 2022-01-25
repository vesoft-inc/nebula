/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/MetaVersionMan.h"

#include "common/fs/FileUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobUtils.h"
#include "meta/upgrade/MetaDataUpgrade.h"
#include "meta/upgrade/v1/MetaServiceUtilsV1.h"
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

// static
Status MetaVersionMan::updateMetaV1ToV2(kvstore::KVEngine* engine) {
  CHECK_NOTNULL(engine);
  auto snapshot = folly::sformat("META_UPGRADE_SNAPSHOT_{}", MetaKeyUtils::genTimestampStr());

  std::string path = folly::sformat("{}/checkpoints/{}", engine->getDataRoot(), snapshot);
  if (!fs::FileUtils::exist(path) && !fs::FileUtils::makeDir(path)) {
    LOG(ERROR) << "Make checkpoint dir: " << path << " failed";
    return Status::Error("Create snapshot file failed");
  }

  std::string dataPath = folly::sformat("{}/data", path);
  auto code = engine->createCheckpoint(dataPath);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Create snapshot failed: " << snapshot;
    return Status::Error("Create snapshot failed");
  }

  auto status = doUpgradeV1ToV2(engine);
  if (!status.ok()) {
    // rollback by snapshot
    return status;
  }

  // delete snapshot file
  auto checkpointPath = folly::sformat("{}/checkpoints/{}", engine->getDataRoot(), snapshot);

  if (fs::FileUtils::exist(checkpointPath) && !fs::FileUtils::remove(checkpointPath.data(), true)) {
    LOG(ERROR) << "Delete snapshot: " << snapshot << " failed, You need to delete it manually";
  }
  return Status::OK();
}

Status MetaVersionMan::updateMetaV2ToV3(kvstore::KVEngine* engine) {
  CHECK_NOTNULL(engine);
  auto snapshot = folly::sformat("META_UPGRADE_SNAPSHOT_{}", MetaKeyUtils::genTimestampStr());

  std::string path = folly::sformat("{}/checkpoints/{}", engine->getDataRoot(), snapshot);
  if (!fs::FileUtils::exist(path) && !fs::FileUtils::makeDir(path)) {
    LOG(ERROR) << "Make checkpoint dir: " << path << " failed";
    return Status::Error("Create snapshot file failed");
  }

  std::string dataPath = folly::sformat("{}/data", path);
  auto code = engine->createCheckpoint(dataPath);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Create snapshot failed: " << snapshot;
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
    LOG(ERROR) << "Delete snapshot: " << snapshot << " failed, You need to delete it manually";
  }
  return Status::OK();
}

// static
Status MetaVersionMan::doUpgradeV1ToV2(kvstore::KVEngine* engine) {
  MetaDataUpgrade upgrader(engine);
  {
    // kSpacesTable
    auto prefix = nebula::meta::v1::kSpacesTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printSpacesV1(iter->val());
        }
        status = upgrader.rewriteSpaces(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kPartsTable
    auto prefix = nebula::meta::v1::kPartsTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printParts(iter->key(), iter->val());
        }
        status = upgrader.rewriteParts(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kHostsTable
    auto prefix = nebula::meta::v1::kHostsTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printHost(iter->key(), iter->val());
        }
        status = upgrader.rewriteHosts(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kLeadersTable
    auto prefix = nebula::meta::v1::kLeadersTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printLeaders(iter->key());
        }
        status = upgrader.rewriteLeaders(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kTagsTable
    auto prefix = nebula::meta::v1::kTagsTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printSchemas(iter->val());
        }
        status = upgrader.rewriteSchemas(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kEdgesTable
    auto prefix = nebula::meta::v1::kEdgesTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printSchemas(iter->val());
        }
        status = upgrader.rewriteSchemas(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kIndexesTable
    auto prefix = nebula::meta::v1::kIndexesTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printIndexes(iter->val());
        }
        status = upgrader.rewriteIndexes(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kConfigsTable
    auto prefix = nebula::meta::v1::kConfigsTable;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (FLAGS_print_info) {
          upgrader.printConfigs(iter->key(), iter->val());
        }
        status = upgrader.rewriteConfigs(iter->key(), iter->val());
        if (!status.ok()) {
          LOG(ERROR) << status;
          return status;
        }
        iter->next();
      }
    }
  }

  {
    // kJob
    auto prefix = JobUtil::jobPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      Status status = Status::OK();
      while (iter->valid()) {
        if (JobDescription::isJobKey(iter->key())) {
          if (FLAGS_print_info) {
            upgrader.printJobDesc(iter->key(), iter->val());
          }
          status = upgrader.rewriteJobDesc(iter->key(), iter->val());
          if (!status.ok()) {
            LOG(ERROR) << status;
            return status;
          }
        } else {
          // the job key format is change, need to delete the old format
          status = upgrader.deleteKeyVal(iter->key());
          if (!status.ok()) {
            LOG(ERROR) << status;
            return status;
          }
        }
        iter->next();
      }
    }
  }

  // delete
  {
    std::vector<std::string> prefixes({nebula::meta::v1::kIndexStatusTable,
                                       nebula::meta::v1::kDefaultTable,
                                       nebula::meta::v1::kCurrJob,
                                       nebula::meta::v1::kJobArchive});
    std::unique_ptr<kvstore::KVIterator> iter;
    for (auto& prefix : prefixes) {
      auto ret = engine->prefix(prefix, &iter);
      if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
        Status status = Status::OK();
        while (iter->valid()) {
          if (prefix == nebula::meta::v1::kIndexesTable) {
            if (FLAGS_print_info) {
              upgrader.printIndexes(iter->val());
            }
            auto oldItem = meta::v1::MetaServiceUtilsV1::parseIndex(iter->val());
            auto spaceId = MetaKeyUtils::parseIndexesKeySpaceID(iter->key());
            status = upgrader.deleteKeyVal(
                MetaKeyUtils::indexIndexKey(spaceId, oldItem.get_index_name()));
            if (!status.ok()) {
              LOG(ERROR) << status;
              return status;
            }
          }
          status = upgrader.deleteKeyVal(iter->key());
          if (!status.ok()) {
            LOG(ERROR) << status;
            return status;
          }
          iter->next();
        }
      }
    }
  }
  if (!setMetaVersionToKV(engine, MetaVersion::V2)) {
    return Status::Error("Persist meta version failed");
  } else {
    return Status::OK();
  }
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
      LOG(ERROR) << "Get zones failed";
      return Status::Error("Get zones failed");
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
      LOG(ERROR) << "Get active hosts failed";
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
      LOG(ERROR) << status;
      return status;
    }
  }

  // Step 2: Update Create space properties about Group
  {
    const auto& prefix = MetaKeyUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = engine->prefix(prefix, &iter);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Get spaces failed";
      return Status::Error("Get spaces failed");
    }

    while (iter->valid()) {
      if (FLAGS_print_info) {
        upgrader.printSpacesV2(iter->val());
      }
      auto status = upgrader.rewriteSpacesV2ToV3(iter->key(), iter->val());
      if (!status.ok()) {
        LOG(ERROR) << status;
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
