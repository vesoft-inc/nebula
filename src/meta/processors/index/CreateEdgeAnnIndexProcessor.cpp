/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/CreateEdgeAnnIndexProcessor.h"

#include <folly/String.h>

#include "common/base/CommonMacro.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

void CreateEdgeAnnIndexProcessor::process(const cpp2::CreateEdgeAnnIndexReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  const auto& indexName = req.get_index_name();
  auto& edgeNames = req.get_edge_names();
  const auto& field = req.get_field();
  auto ifNotExists = req.get_if_not_exists();

  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());

  // check if the space has the index with the same name
  auto ret = getIndexID(space, indexName);
  if (nebula::ok(ret)) {
    if (ifNotExists) {
      handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    } else {
      LOG(INFO) << "Create Edge Index Failed: " << indexName << " has existed";
      handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
    }
    resp_.id_ref() = to(nebula::value(ret), EntryType::INDEX);
    onFinished();
    return;
  } else {
    auto retCode = nebula::error(ret);
    if (retCode != nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
      LOG(INFO) << "Create Edge Index Failed, index name " << indexName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
  }

  const auto& prefix = MetaKeyUtils::indexPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Edge indexes prefix failed, space id " << space
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto checkIter = nebula::value(iterRet).get();

  // check if the Edge index with the same fields exist
  while (checkIter->valid()) {
    auto val = checkIter->val();
    auto item = MetaKeyUtils::parseAnnIndex(val);

    if (checkAnnIndexExist(edgeNames, field, item)) {
      if (ifNotExists) {
        resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
        cpp2::ID thriftID;
        // Fill index id to avoid broken promise
        thriftID.index_id_ref() = item.get_index_id();
        resp_.id_ref() = thriftID;
      } else {
        resp_.code_ref() = nebula::cpp2::ErrorCode::E_EXISTED;
      }
      onFinished();
      return;
    }
    checkIter->next();
  }
  std::vector<cpp2::ColumnDef> columns;
  std::vector<nebula::cpp2::SchemaID> schemaIDs;
  for (auto& edgeName : edgeNames) {
    auto edgeTypeRet = getEdgeType(space, edgeName);
    if (!nebula::ok(edgeTypeRet)) {
      auto retCode = nebula::error(edgeTypeRet);
      LOG(INFO) << "Create Edge Index Failed, Edge " << edgeName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
    auto edgeType = nebula::value(edgeTypeRet);
    nebula::cpp2::SchemaID schemaID;
    schemaID.edge_type_ref() = edgeType;

    auto schemaRet = getLatestEdgeSchema(space, edgeType);
    if (!nebula::ok(schemaRet)) {
      auto retCode = nebula::error(schemaRet);
      LOG(INFO) << "Get Edge schema failed, space id " << space << " EdgeName " << edgeName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }

    // check if all the given fields valid for building index in latest Edge schema
    auto latestEdgeSchema = std::move(nebula::value(schemaRet));
    const auto& schemaCols = latestEdgeSchema.get_columns();

    auto iter = std::find_if(schemaCols.begin(), schemaCols.end(), [field](const auto& col) {
      return field.get_name() == col.get_name();
    });
    if (iter == schemaCols.end()) {
      LOG(INFO) << "Field " << field.get_name() << " not found in Edge " << edgeName;
      handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
      onFinished();
      return;
    }
    cpp2::ColumnDef col = *iter;
    if (col.type.get_type() != nebula::cpp2::PropertyType::VECTOR) {
      LOG(INFO) << "Field " << field.get_name() << " in Edge " << edgeName << " is not vector."
                << "It can not be ann indexed.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    }
    columns.emplace_back(col);
    schemaIDs.emplace_back(schemaID);
  }

  std::vector<kvstore::KV> data;
  auto edgeIndexRet = autoIncrementIdInSpace(space);
  if (!nebula::ok(edgeIndexRet)) {
    LOG(INFO) << "Create Edge index failed : Get Edge index ID failed";
    handleErrorCode(nebula::error(edgeIndexRet));
    onFinished();
    return;
  }

  auto edgeIndex = nebula::value(edgeIndexRet);
  cpp2::AnnIndexItem item;
  item.index_id_ref() = edgeIndex;
  item.index_name_ref() = indexName;
  item.prop_name_ref() = field.get_name();
  item.schema_ids_ref() = std::move(schemaIDs);
  item.schema_names_ref() = edgeNames;
  item.fields_ref() = std::move(columns);
  if (req.comment_ref().has_value()) {
    item.comment_ref() = *req.comment_ref();
  }
  if (req.ann_params_ref().has_value()) {
    item.ann_params_ref() = *req.ann_params_ref();
  }

  data.emplace_back(MetaKeyUtils::indexIndexKey(space, indexName),
                    std::string(reinterpret_cast<const char*>(&edgeIndex), sizeof(IndexID)));
  data.emplace_back(MetaKeyUtils::indexKey(space, edgeIndex), MetaKeyUtils::annIndexVal(item));
  LOG(INFO) << "Create Edge Ann Index " << indexName << ", EdgeIndex " << edgeIndex;
  LOG(ERROR) << "Create Edge Ann Index " << indexName << ", EdgeIndex " << edgeIndex
             << ", indexKey " << folly::hexlify(MetaKeyUtils::indexKey(space, edgeIndex));
  resp_.id_ref() = to(edgeIndex, EntryType::INDEX);
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
