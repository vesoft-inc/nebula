/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/CreateTagAnnIndexProcessor.h"

#include "common/base/CommonMacro.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

void CreateTagAnnIndexProcessor::process(const cpp2::CreateTagAnnIndexReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  const auto& indexName = req.get_index_name();
  auto& tagNames = req.get_tag_names();
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
      LOG(INFO) << "Create Tag Index Failed: " << indexName << " has existed";
      handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
    }
    resp_.id_ref() = to(nebula::value(ret), EntryType::INDEX);
    onFinished();
    return;
  } else {
    auto retCode = nebula::error(ret);
    if (retCode != nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
      LOG(INFO) << "Create Tag Index Failed, index name " << indexName
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
    LOG(INFO) << "Tag indexes prefix failed, space id " << space
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto checkIter = nebula::value(iterRet).get();

  // check if the tag index with the same fields exist
  while (checkIter->valid()) {
    auto val = checkIter->val();
    auto item = MetaKeyUtils::parseAnnIndex(val);

    if (checkAnnIndexExist(tagNames, field, item)) {
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
  for (auto& tagName : tagNames) {
    auto tagIDRet = getTagId(space, tagName);
    if (!nebula::ok(tagIDRet)) {
      auto retCode = nebula::error(tagIDRet);
      LOG(INFO) << "Create Tag Index Failed, Tag " << tagName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
    auto tagID = nebula::value(tagIDRet);
    nebula::cpp2::SchemaID schemaID;
    schemaID.tag_id_ref() = tagID;

    auto schemaRet = getLatestTagSchema(space, tagID);
    if (!nebula::ok(schemaRet)) {
      auto retCode = nebula::error(schemaRet);
      LOG(INFO) << "Get tag schema failed, space id " << space << " tagName " << tagName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }

    // check if all the given fields valid for building index in latest tag schema
    auto latestTagSchema = std::move(nebula::value(schemaRet));
    const auto& schemaCols = latestTagSchema.get_columns();

    auto iter = std::find_if(schemaCols.begin(), schemaCols.end(), [field](const auto& col) {
      return field.get_name() == col.get_name();
    });
    if (iter == schemaCols.end()) {
      LOG(INFO) << "Field " << field.get_name() << " not found in Tag " << tagName;
      handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
      onFinished();
      return;
    }
    cpp2::ColumnDef col = *iter;
    if (col.type.get_type() != nebula::cpp2::PropertyType::VECTOR) {
      LOG(INFO) << "Field " << field.get_name() << " in Tag " << tagName << " is not vector."
                << "It can not be ann indexed.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    }
    columns.emplace_back(col);
    schemaIDs.emplace_back(schemaID);
  }

  std::vector<kvstore::KV> data;
  auto tagIndexRet = autoIncrementIdInSpace(space);
  if (!nebula::ok(tagIndexRet)) {
    LOG(INFO) << "Create tag index failed : Get tag index ID failed";
    handleErrorCode(nebula::error(tagIndexRet));
    onFinished();
    return;
  }

  auto tagIndex = nebula::value(tagIndexRet);
  cpp2::AnnIndexItem item;
  item.index_id_ref() = tagIndex;
  item.index_name_ref() = indexName;
  item.prop_name_ref() = field.get_name();
  item.schema_ids_ref() = std::move(schemaIDs);
  item.schema_names_ref() = tagNames;
  item.fields_ref() = std::move(columns);
  if (req.comment_ref().has_value()) {
    item.comment_ref() = *req.comment_ref();
  }
  if (req.ann_params_ref().has_value()) {
    item.ann_params_ref() = *req.ann_params_ref();
  }

  data.emplace_back(MetaKeyUtils::indexIndexKey(space, indexName),
                    std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(IndexID)));
  data.emplace_back(MetaKeyUtils::indexKey(space, tagIndex), MetaKeyUtils::annIndexVal(item));
  LOG(INFO) << "Create Tag Ann Index " << indexName << ", tagIndex " << tagIndex;
  resp_.id_ref() = to(tagIndex, EntryType::INDEX);
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
