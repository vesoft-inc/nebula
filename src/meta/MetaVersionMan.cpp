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
    return (iter->key().size() == v1KeySize) ? MetaVersion::V1 : MetaVersion::V3_4;
  }
  // No hosts exists, regard as version 3
  return MetaVersion::V3_4;
}

// static
bool MetaVersionMan::setMetaVersionToKV(kvstore::KVEngine* engine, MetaVersion version) {
  CHECK_NOTNULL(engine);
  std::string versionValue =
      std::string(reinterpret_cast<const char*>(&version), sizeof(MetaVersion));
  auto code = engine->put(kMetaVersionKey, std::move(versionValue));
  return code == nebula::cpp2::ErrorCode::SUCCEEDED;
}

Status MetaVersionMan::updateMetaV3ToV3_4(kvstore::KVEngine* engine) {
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

  auto status = doUpgradeV3ToV3_4(engine);
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

Status MetaVersionMan::doUpgradeV3ToV3_4(kvstore::KVEngine* engine) {
  std::unique_ptr<kvstore::KVIterator> fulltextIter;
  auto code = engine->prefix(MetaKeyUtils::fulltextIndexPrefix(), &fulltextIter);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Upgrade meta failed";
    return Status::Error("Update meta failed");
  }
  std::vector<std::string> fulltextList;
  while (fulltextIter->valid()) {
    fulltextList.push_back(fulltextIter->key().toString());
    fulltextIter->next();
  }
  code = engine->multiRemove(fulltextList);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Upgrade meta failed";
    return Status::Error("Upgrade meta failed");
  }

  if (!setMetaVersionToKV(engine, MetaVersion::V3_4)) {
    return Status::Error("Persist meta version failed");
  } else {
    return Status::OK();
  }
}

}  // namespace meta
}  // namespace nebula
