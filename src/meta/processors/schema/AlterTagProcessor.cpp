/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/AlterTagProcessor.h"

#include "meta/processors/schema/SchemaUtil.h"

namespace nebula {
namespace meta {

void AlterTagProcessor::process(const cpp2::AlterTagReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& tagName = req.get_tag_name();

  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto ret = getTagId(spaceId, tagName);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(INFO) << "Failed to get tag " << tagName << " error "
              << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto tagId = nebula::value(ret);

  // Check the tag belongs to the space, and get the shcema with latest version
  auto tagPrefix = MetaKeyUtils::schemaTagPrefix(spaceId, tagId);
  auto retPre = doPrefix(tagPrefix);
  if (!nebula::ok(retPre)) {
    auto retCode = nebula::error(retPre);
    LOG(INFO) << "Tag Prefix failed, tagname: " << tagName << ", spaceId " << spaceId
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto iter = nebula::value(retPre).get();
  if (!iter->valid()) {
    LOG(INFO) << "Tag could not be found, spaceId " << spaceId << ", tagname: " << tagName;
    handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
    onFinished();
    return;
  }

  std::unordered_map<SchemaVer, folly::StringPiece> schemasRaw;
  auto latestVersion = MetaKeyUtils::getLatestTagScheInfo(iter, schemasRaw);
  auto newVersion = latestVersion + 1;
  auto schema = MetaKeyUtils::parseSchema(schemasRaw[latestVersion]);
  auto columns = schema.get_columns();
  auto prop = schema.get_schema_prop();

  std::vector<std::vector<cpp2::ColumnDef>> allVersionedColumns;
  for (auto entry : schemasRaw) {
    allVersionedColumns.emplace_back(MetaKeyUtils::parseSchema(entry.second).get_columns());
  }

  // Update schema column
  auto& tagItems = req.get_tag_items();

  auto iCode = getIndexes(spaceId, tagId);
  if (!nebula::ok(iCode)) {
    handleErrorCode(nebula::error(iCode));
    onFinished();
    return;
  }

  auto indexes = std::move(nebula::value(iCode));

  auto existIndex = !indexes.empty();
  if (existIndex) {
    auto iStatus = indexCheck(indexes, tagItems);
    if (iStatus != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Alter tag error, index conflict : "
                << apache::thrift::util::enumNameSafe(iStatus);
      handleErrorCode(iStatus);
      onFinished();
      return;
    }
  }
  // Check fulltext index
  auto ftIdxRet = getFTIndex(spaceId, tagId);
  bool existFTIndex = false;
  if (nebula::ok(ftIdxRet)) {
    auto fti = std::move(nebula::value(ftIdxRet));
    existFTIndex = !fti.empty();
    auto ftStatus = ftIndexCheck(fti, tagItems);
    if (ftStatus != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleErrorCode(ftStatus);
      onFinished();
      return;
    }
  } else if (nebula::error(ftIdxRet) != nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
    handleErrorCode(nebula::error(ftIdxRet));
    onFinished();
    return;
  }

  auto& alterSchemaProp = req.get_schema_prop();

  // If index exist, could not alter ttl column
  if (existIndex || existFTIndex) {
    int64_t duration = 0;
    if (alterSchemaProp.get_ttl_duration()) {
      duration = *alterSchemaProp.get_ttl_duration();
    }
    std::string col;
    if (alterSchemaProp.get_ttl_col()) {
      col = *alterSchemaProp.get_ttl_col();
    }
    if (!col.empty() && duration > 0) {
      LOG(INFO) << "Alter tag error, index and ttl conflict";
      handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
      onFinished();
      return;
    }
  }

  for (auto& tagItem : tagItems) {
    auto& cols = tagItem.get_schema().get_columns();
    for (auto& col : cols) {
      auto retCode = MetaServiceUtils::alterColumnDefs(
          columns, prop, col, *tagItem.op_ref(), allVersionedColumns);
      if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "Alter tag column error " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
      }
    }
  }

  if (!SchemaUtil::checkType(columns)) {
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  // Update schema property if tag not index
  auto retCode = MetaServiceUtils::alterSchemaProp(columns, prop, alterSchemaProp, existIndex);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Alter tag property error " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  schema.schema_prop_ref() = std::move(prop);
  schema.columns_ref() = std::move(columns);

  std::vector<kvstore::KV> data;
  LOG(INFO) << "Alter Tag " << tagName << ", tagId " << tagId << ", new version " << newVersion;
  data.emplace_back(MetaKeyUtils::schemaTagKey(spaceId, tagId, newVersion),
                    MetaKeyUtils::schemaVal(tagName, schema));
  resp_.id_ref() = to(tagId, EntryType::TAG);
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
