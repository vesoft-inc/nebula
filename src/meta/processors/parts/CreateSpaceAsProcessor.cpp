/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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

  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto oldSpaceName = req.get_old_space_name();
  auto newSpaceName = req.get_new_space_name();
  auto oldSpaceId = getSpaceId(oldSpaceName);
  auto newSpaceId = getSpaceId(newSpaceName);

  if (!nebula::ok(oldSpaceId)) {
    rc_ = nebula::error(oldSpaceId);
    LOG(INFO) << "Create Space [" << newSpaceName << "] as [" << oldSpaceName
              << "] failed. Old space does not exists. rc = "
              << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  if (nebula::ok(newSpaceId)) {
    if (req.get_if_not_exists()) {
      rc_ = nebula::cpp2::ErrorCode::SUCCEEDED;
      cpp2::ID id;
      id.space_id_ref() = static_cast<GraphSpaceID>(nebula::value(newSpaceId));
      resp_.id_ref() = id;
      return;
    }

    rc_ = nebula::cpp2::ErrorCode::E_EXISTED;
    LOG(INFO) << "Create Space [" << newSpaceName << "] as [" << oldSpaceName
              << "] failed. New space already exists.";
    return;
  }

  newSpaceId = autoIncrementId();
  if (!nebula::ok(newSpaceId)) {
    rc_ = nebula::error(newSpaceId);
    LOG(INFO) << "Create Space Failed : Generate new space id failed";
    return;
  }

  std::vector<kvstore::KV> data;
  auto newSpaceData =
      makeNewSpaceData(nebula::value(oldSpaceId), nebula::value(newSpaceId), newSpaceName);
  if (nebula::ok(newSpaceData)) {
    data.insert(data.end(), nebula::value(newSpaceData).begin(), nebula::value(newSpaceData).end());
  } else {
    rc_ = nebula::error(newSpaceData);
    LOG(INFO) << "Make new space data failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  auto newTags = makeNewTags(nebula::value(oldSpaceId), nebula::value(newSpaceId));
  if (nebula::ok(newTags)) {
    data.insert(data.end(), nebula::value(newTags).begin(), nebula::value(newTags).end());
  } else {
    rc_ = nebula::error(newTags);
    LOG(INFO) << "Make new tags failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  auto newEdges = makeNewEdges(nebula::value(oldSpaceId), nebula::value(newSpaceId));
  if (nebula::ok(newEdges)) {
    data.insert(data.end(), nebula::value(newEdges).begin(), nebula::value(newEdges).end());
  } else {
    rc_ = nebula::error(newEdges);
    LOG(INFO) << "Make new edges failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  auto newIndexes = makeNewIndexes(nebula::value(oldSpaceId), nebula::value(newSpaceId));
  if (nebula::ok(newIndexes)) {
    data.insert(data.end(), nebula::value(newIndexes).begin(), nebula::value(newIndexes).end());
  } else {
    rc_ = nebula::error(newIndexes);
    LOG(INFO) << "Make new indexes failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  auto newLocalIdKey = makeLocalIDKey(nebula::value(oldSpaceId), nebula::value(newSpaceId));
  if (nebula::ok(newLocalIdKey)) {
    data.push_back(nebula::value(newLocalIdKey));
  } else {
    rc_ = nebula::error(newLocalIdKey);
    LOG(INFO) << "Make new local id failed, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }

  resp_.id_ref() = to(nebula::value(newSpaceId), EntryType::SPACE);
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  rc_ = doSyncPut(std::move(data));
  if (rc_ != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Update last update time error, " << apache::thrift::util::enumNameSafe(rc_);
    return;
  }
  LOG(INFO) << "Created space " << newSpaceName;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewSpaceData(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId, const std::string &spaceName) {
  auto oldSpaceKey = MetaKeyUtils::spaceKey(oldSpaceId);
  auto oldSpaceVal = doGet(oldSpaceKey);
  if (!nebula::ok(oldSpaceVal)) {
    LOG(INFO) << "Create Space Failed : Generate new space id failed";
    rc_ = nebula::error(oldSpaceVal);
    return rc_;
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::indexSpaceKey(spaceName),
                    std::string(reinterpret_cast<const char *>(&newSpaceId), sizeof(newSpaceId)));
  cpp2::SpaceDesc spaceDesc = MetaKeyUtils::parseSpace(nebula::value(oldSpaceVal));
  spaceDesc.space_name_ref() = spaceName;
  data.emplace_back(MetaKeyUtils::spaceKey(newSpaceId), MetaKeyUtils::spaceVal(spaceDesc));

  auto prefix = MetaKeyUtils::partPrefix(oldSpaceId);
  auto partPrefix = doPrefix(prefix);
  if (!nebula::ok(partPrefix)) {
    return nebula::error(partPrefix);
  }
  auto iter = nebula::value(partPrefix).get();
  while (iter->valid()) {
    auto partId = MetaKeyUtils::parsePartKeyPartId(iter->key());
    data.emplace_back(MetaKeyUtils::partKey(newSpaceId, partId), iter->val());
    iter->next();
  }
  return data;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewTags(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId) {
  auto prefix = MetaKeyUtils::schemaTagsPrefix(oldSpaceId);
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
  while (iter->valid()) {
    auto val = iter->val();

    // tag name -> tag id
    auto tagId = MetaKeyUtils::parseTagId(iter->key());
    auto tagNameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto tagName = val.subpiece(sizeof(int32_t), tagNameLen).str();
    data.emplace_back(MetaKeyUtils::indexTagKey(newSpaceId, tagName),
                      std::string(reinterpret_cast<const char *>(&tagId), sizeof(tagId)));

    // tag id -> tag schema
    auto tagVer = MetaKeyUtils::parseTagVersion(iter->key());
    auto key = MetaKeyUtils::schemaTagKey(newSpaceId, tagId, tagVer);
    data.emplace_back(std::move(key), val.str());
    iter->next();
  }
  return data;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewEdges(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId) {
  auto prefix = MetaKeyUtils::schemaEdgesPrefix(oldSpaceId);
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
  while (iter->valid()) {
    auto val = iter->val();

    // edge name -> edge id
    auto edgeType = MetaKeyUtils::parseEdgeType(iter->key());
    auto edgeNameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto edgeName = val.subpiece(sizeof(int32_t), edgeNameLen).str();
    data.emplace_back(MetaKeyUtils::indexEdgeKey(newSpaceId, edgeName),
                      std::string(reinterpret_cast<const char *>(&edgeType), sizeof(edgeType)));

    // edge id -> edge schema
    auto ver = MetaKeyUtils::parseEdgeVersion(iter->key());
    auto key = MetaKeyUtils::schemaEdgeKey(newSpaceId, edgeType, ver);
    data.emplace_back(std::move(key), val.str());
    iter->next();
  }
  return data;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> CreateSpaceAsProcessor::makeNewIndexes(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId) {
  auto prefix = MetaKeyUtils::indexPrefix(oldSpaceId);
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
  while (iter->valid()) {
    auto val = iter->val();

    auto indexId = MetaKeyUtils::parseIndexesKeyIndexID(iter->key());

    cpp2::IndexItem idxItem = MetaKeyUtils::parseIndex(val.str());
    auto indexName = idxItem.get_index_name();

    data.emplace_back(MetaKeyUtils::indexIndexKey(newSpaceId, indexName),
                      std::string(reinterpret_cast<const char *>(&indexId), sizeof(indexId)));

    data.emplace_back(MetaKeyUtils::indexKey(newSpaceId, indexId), MetaKeyUtils::indexVal(idxItem));
    iter->next();
  }
  return data;
}

ErrorOr<nebula::cpp2::ErrorCode, kvstore::KV> CreateSpaceAsProcessor::makeLocalIDKey(
    GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId) {
  auto localIdkey = MetaKeyUtils::localIdKey(oldSpaceId);
  int32_t id;
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, localIdkey, &val);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      return ret;
    }

    // In order to be compatible with the existing old schema, and simple to implement,
    // when the local_id record does not exist in space, directly use the smallest
    // id available globally.
    auto globalIdRet = getAvailableGlobalId();
    if (!nebula::ok(globalIdRet)) {
      return nebula::error(globalIdRet);
    }
    id = nebula::value(globalIdRet);
  } else {
    id = *reinterpret_cast<const int32_t *>(val.c_str());
  }
  auto newLocalIdKey = MetaKeyUtils::localIdKey(newSpaceId);
  return {newLocalIdKey, std::string(reinterpret_cast<const char *>(&id), sizeof(id))};
}

}  // namespace meta
}  // namespace nebula
