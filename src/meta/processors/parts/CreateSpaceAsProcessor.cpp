/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/parts/CreateSpaceAsProcessor.h"

#include "meta/ActiveHostsMan.h"

namespace nebula {
namespace meta {

void CreateSpaceAsProcessor::process(const cpp2::CreateSpaceAsReq &req) {
  SCOPE_EXIT {
    if (rc_ != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleErrorCode(rc_);
    }
    onFinished();
  };

  folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
  auto oldSpaceName = req.get_old_space_name();
  auto newSpaceName = req.get_new_space_name();
  auto oldSpaceId = getSpaceId(oldSpaceName);
  auto newSpaceId = getSpaceId(newSpaceName);

  if (!nebula::ok(oldSpaceId)) {
    rc_ = nebula::error(oldSpaceId);
    LOG(ERROR) << "Create Space [" << newSpaceName << "] as [" << oldSpaceName
               << "] failed. rc = " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  if (nebula::ok(newSpaceId)) {
    if (!req.get_if_not_exists()) {
      LOG(ERROR) << "Create Space " << newSpaceName << " as " << oldSpaceName << "failed. "
                 << newSpaceName << " already exist";
      rc_ = nebula::cpp2::ErrorCode::E_EXISTED;
      return;
    }
  }

  newSpaceId = autoIncrementId();
  if (!nebula::ok(newSpaceId)) {
    rc_ = nebula::error(newSpaceId);
    LOG(ERROR) << "Create Space Failed : Generate new space id failed";
    return;
  }

  auto newSpaceData =
      makeNewSpaceData(nebula::value(oldSpaceId), nebula::value(newSpaceId), newSpaceName);
  if (!nebula::ok(newSpaceData)) {
    rc_ = nebula::error(newSpaceData);
    LOG(ERROR) << "make new space data failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  auto newTags = makeNewTags(nebula::value(oldSpaceId), nebula::value(newSpaceId));
  if (!nebula::ok(newTags)) {
    rc_ = nebula::error(newTags);
    LOG(ERROR) << "make new tags failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  auto newEdges = makeNewEdges(nebula::value(oldSpaceId), nebula::value(newSpaceId));
  if (!nebula::ok(newEdges)) {
    rc_ = nebula::error(newEdges);
    LOG(ERROR) << "make new edges failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  auto newIndexes = makeNewIndexes(nebula::value(oldSpaceId), nebula::value(newSpaceId));
  if (!nebula::ok(newIndexes)) {
    rc_ = nebula::error(newIndexes);
    LOG(ERROR) << "make new indexes failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  std::vector<kvstore::KV> data;
  data.insert(data.end(), nebula::value(newSpaceData).begin(), nebula::value(newSpaceData).end());
  data.insert(data.end(), nebula::value(newTags).begin(), nebula::value(newTags).end());
  data.insert(data.end(), nebula::value(newEdges).begin(), nebula::value(newEdges).end());
  data.insert(data.end(), nebula::value(newIndexes).begin(), nebula::value(newIndexes).end());

  resp_.set_id(to(nebula::value(newSpaceId), EntryType::SPACE));
  rc_ = doSyncPut(std::move(data));
  if (rc_ != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "put data error, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  rc_ = LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec());
  if (rc_ != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "update last update time error, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }
  LOG(INFO) << "created space " << newSpaceName;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewSpaceData(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId, const std::string &spaceName) {
  auto oldSpaceKey = MetaServiceUtils::spaceKey(oldSpaceId);
  auto oldSpaceVal = doGet(oldSpaceKey);
  if (!nebula::ok(oldSpaceVal)) {
    LOG(ERROR) << "Create Space Failed : Generate new space id failed";
    rc_ = nebula::error(oldSpaceVal);
    return rc_;
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(MetaServiceUtils::indexSpaceKey(spaceName),
                    std::string(reinterpret_cast<const char *>(&newSpaceId), sizeof(newSpaceId)));
  cpp2::SpaceDesc spaceDesc = MetaServiceUtils::parseSpace(nebula::value(oldSpaceVal));
  spaceDesc.set_space_name(spaceName);
  data.emplace_back(MetaServiceUtils::spaceKey(newSpaceId), MetaServiceUtils::spaceVal(spaceDesc));
  return data;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewTags(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
  auto prefix = MetaServiceUtils::schemaTagsPrefix(oldSpaceId);
  auto tagPrefix = doPrefix(prefix);
  if (!nebula::ok(tagPrefix)) {
    if (nebula::error(tagPrefix) == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      // no tag is ok.
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    return nebula::error(tagPrefix);
  }

  std::vector<kvstore::KV> data;
  auto iter = nebula::value(tagPrefix).get();
  for (; iter->valid(); iter->next()) {
    auto val = iter->val();

    auto tagId = autoIncrementIdInSpace(newSpaceId);
    if (!nebula::ok(tagId)) {
      return nebula::error(tagId);
    }
    auto vTagID = nebula::value(tagId);

    auto tagNameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto tagName = val.subpiece(sizeof(int32_t), tagNameLen).str();
    data.emplace_back(MetaServiceUtils::indexTagKey(newSpaceId, tagName),
                      std::string(reinterpret_cast<const char *>(&vTagID), sizeof(vTagID)));

    auto tagVer = MetaServiceUtils::parseTagVersion(iter->key());
    auto key = MetaServiceUtils::schemaTagKey(newSpaceId, vTagID, tagVer);
    data.emplace_back(std::move(key), val.str());
  }
  return data;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewEdges(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
  auto prefix = MetaServiceUtils::schemaEdgesPrefix(oldSpaceId);
  auto edgePrefix = doPrefix(prefix);
  if (!nebula::ok(edgePrefix)) {
    if (nebula::error(edgePrefix) == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      // no edge is ok.
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    return nebula::error(edgePrefix);
  }

  std::vector<kvstore::KV> data;
  auto iter = nebula::value(edgePrefix).get();
  for (; iter->valid(); iter->next()) {
    auto val = iter->val();

    auto edgeType = autoIncrementIdInSpace(newSpaceId);
    if (!nebula::ok(edgeType)) {
      return nebula::error(edgeType);
    }
    auto vEdgeType = nebula::value(edgeType);

    auto edgeNameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto edgeName = val.subpiece(sizeof(int32_t), edgeNameLen).str();
    data.emplace_back(MetaServiceUtils::indexTagKey(newSpaceId, edgeName),
                      std::string(reinterpret_cast<const char *>(&vEdgeType), sizeof(vEdgeType)));

    auto ver = MetaServiceUtils::parseEdgeVersion(iter->key());
    auto key = MetaServiceUtils::schemaEdgeKey(newSpaceId, vEdgeType, ver);
    data.emplace_back(std::move(key), val.str());
  }
  return data;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewIndexes(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
  auto prefix = MetaServiceUtils::indexPrefix(oldSpaceId);
  auto indexPrefix = doPrefix(prefix);
  if (!nebula::ok(indexPrefix)) {
    if (nebula::error(indexPrefix) == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      // no index is ok.
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    return nebula::error(indexPrefix);
  }

  std::vector<kvstore::KV> data;
  auto iter = nebula::value(indexPrefix).get();
  for (; iter->valid(); iter->next()) {
    auto val = iter->val();

    auto indexId = autoIncrementIdInSpace(newSpaceId);
    if (!nebula::ok(indexId)) {
      return nebula::error(indexId);
    }
    auto vIndexId = nebula::value(indexId);

    cpp2::IndexItem idxItem = MetaServiceUtils::parseIndex(val.str());
    auto indexName = idxItem.get_index_name();
    idxItem.set_index_id(vIndexId);

    data.emplace_back(MetaServiceUtils::indexIndexKey(newSpaceId, indexName),
                      std::string(reinterpret_cast<const char *>(&vIndexId), sizeof(vIndexId)));

    data.emplace_back(MetaServiceUtils::indexKey(newSpaceId, vIndexId),
                      MetaServiceUtils::indexVal(idxItem));
  }
  return data;
}

}  // namespace meta
}  // namespace nebula
