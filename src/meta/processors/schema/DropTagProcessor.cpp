/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/DropTagProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void DropTagProcessor::process(const cpp2::DropTagReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& tagName = req.get_tag_name();

  TagID tagId;
  auto indexKey = MetaKeyUtils::indexTagKey(spaceId, tagName);
  auto iRet = doGet(indexKey);
  if (nebula::ok(iRet)) {
    tagId = *reinterpret_cast<const TagID*>(nebula::value(iRet).c_str());
    resp_.id_ref() = to(tagId, EntryType::TAG);
  } else {
    auto retCode = nebula::error(iRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(INFO) << "Drop tag failed :" << tagName << " not found.";
        retCode = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
      }
    } else {
      LOG(INFO) << "Get Tag failed, tag name " << tagName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto indexes = getIndexes(spaceId, tagId);
  if (!nebula::ok(indexes)) {
    handleErrorCode(nebula::error(indexes));
    onFinished();
    return;
  }
  if (!nebula::value(indexes).empty()) {
    LOG(INFO) << "Drop tag error, index conflict, please delete index first.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS);
    onFinished();
    return;
  }

  auto ftIdxRet = getFTIndex(spaceId, tagId);
  if (nebula::ok(ftIdxRet)) {
    if (!nebula::value(ftIdxRet).empty()) {
      LOG(INFO) << "Drop tag error, fulltext index conflict, "
                << "please delete fulltext index first.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS);
      onFinished();
      return;
    }
  } else {
    handleErrorCode(nebula::error(ftIdxRet));
    onFinished();
    return;
  }

  auto ret = getTagKeys(spaceId, tagId);
  if (!nebula::ok(ret)) {
    handleErrorCode(nebula::error(ret));
    onFinished();
    return;
  }

  auto keys = nebula::value(ret);
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  for (auto key : keys) {
    batchHolder->remove(std::move(key));
  }
  batchHolder->remove(std::move(indexKey));

  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
  LOG(INFO) << "Drop Tag " << tagName;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> DropTagProcessor::getTagKeys(
    GraphSpaceID id, TagID tagId) {
  std::vector<std::string> keys;
  auto key = MetaKeyUtils::schemaTagPrefix(id, tagId);
  auto iterRet = doPrefix(key);
  if (!nebula::ok(iterRet)) {
    LOG(INFO) << "Tag schema prefix failed, tag id " << tagId;
    return nebula::error(iterRet);
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    keys.emplace_back(iter->key());
    iter->next();
  }
  return keys;
}

}  // namespace meta
}  // namespace nebula
