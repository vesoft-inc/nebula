/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/AlterEdgeProcessor.h"

#include "meta/processors/schema/SchemaUtil.h"

namespace nebula {
namespace meta {

void AlterEdgeProcessor::process(const cpp2::AlterEdgeReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& edgeName = req.get_edge_name();

  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto ret = getEdgeType(spaceId, edgeName);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(INFO) << "Failed to get edge " << edgeName << " error "
              << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto edgeType = nebula::value(ret);

  // Check the edge belongs to the space
  // Because there are many edge types with same type and different versions, we should get
  // latest edge type by prefix scanning.
  auto edgePrefix = MetaKeyUtils::schemaEdgePrefix(spaceId, edgeType);
  auto retPre = doPrefix(edgePrefix);
  if (!nebula::ok(retPre)) {
    auto retCode = nebula::error(retPre);
    LOG(INFO) << "Edge Prefix failed, edgename: " << edgeName << ", spaceId " << spaceId
              << " error " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto iter = nebula::value(retPre).get();
  if (!iter->valid()) {
    LOG(INFO) << "Edge could not be found, spaceId " << spaceId << ", edgename: " << edgeName;
    handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
    onFinished();
    return;
  }

  // Parse version from edge type key
  std::unordered_map<SchemaVer, folly::StringPiece> schemasRaw;
  auto latestVersion = MetaKeyUtils::getLatestEdgeScheInfo(iter, schemasRaw);
  auto newVersion = latestVersion + 1;
  auto schema = MetaKeyUtils::parseSchema(schemasRaw[latestVersion]);
  auto columns = schema.get_columns();
  auto prop = schema.get_schema_prop();

  std::vector<std::vector<cpp2::ColumnDef>> allVersionedColumns;
  for (auto entry : schemasRaw) {
    allVersionedColumns.emplace_back(MetaKeyUtils::parseSchema(entry.second).get_columns());
  }

  // Update schema column
  auto& edgeItems = req.get_edge_items();

  // Index check: could drop or change property having index
  auto iRet = getIndexes(spaceId, edgeType);
  if (!nebula::ok(iRet)) {
    handleErrorCode(nebula::error(iRet));
    onFinished();
    return;
  }
  auto indexes = std::move(nebula::value(iRet));
  auto existIndex = !indexes.empty();
  if (existIndex) {
    auto iStatus = indexCheck(indexes, edgeItems);
    if (iStatus != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Alter edge error, index conflict : "
                << apache::thrift::util::enumNameSafe(iStatus);
      handleErrorCode(iStatus);
      onFinished();
      return;
    }
  }

  // If index exist, could not alter ttl column. Because the old indexes either already
  // set ttl according to the old ttl column or do not have any ttl.
  auto& alterSchemaProp = req.get_schema_prop();
  if (existIndex) {
    int64_t duration = 0;
    if (alterSchemaProp.get_ttl_duration()) {
      duration = *alterSchemaProp.get_ttl_duration();
    }
    std::string col;
    if (alterSchemaProp.get_ttl_col()) {
      col = *alterSchemaProp.get_ttl_col();
    }
    if (!col.empty() && duration > 0) {
      LOG(INFO) << "Alter edge error, index and ttl conflict";
      handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
      onFinished();
      return;
    }
  }

  // Check fulltext index
  auto ftIdxRet = getFTIndex(spaceId, edgeType);
  if (nebula::ok(ftIdxRet)) {
    auto fti = std::move(nebula::value(ftIdxRet));
    auto ftStatus = ftIndexCheck(fti, edgeItems);
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

  for (auto& edgeItem : edgeItems) {
    auto& cols = edgeItem.get_schema().get_columns();
    for (auto& col : cols) {
      auto retCode = MetaServiceUtils::alterColumnDefs(
          columns, prop, col, *edgeItem.op_ref(), allVersionedColumns, true);
      if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "Alter edge column error " << apache::thrift::util::enumNameSafe(retCode);
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

  // Update schema property if edge not index
  auto retCode =
      MetaServiceUtils::alterSchemaProp(columns, prop, alterSchemaProp, existIndex, true);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Alter edge property error " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  schema.schema_prop_ref() = std::move(prop);
  schema.columns_ref() = std::move(columns);

  std::vector<kvstore::KV> data;
  LOG(INFO) << "Alter edge " << edgeName << ", edgeType " << edgeType << ", new version "
            << newVersion;
  data.emplace_back(MetaKeyUtils::schemaEdgeKey(spaceId, edgeType, newVersion),
                    MetaKeyUtils::schemaVal(edgeName, schema));
  resp_.id_ref() = to(edgeType, EntryType::EDGE);
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
