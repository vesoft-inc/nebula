/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/CreateTagIndexProcessor.h"

#include "common/base/CommonMacro.h"

namespace nebula {
namespace meta {

void CreateTagIndexProcessor::process(const cpp2::CreateTagIndexReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  const auto& indexName = req.get_index_name();
  auto& tagName = req.get_tag_name();
  const auto& fields = req.get_fields();
  auto ifNotExists = req.get_if_not_exists();

  std::set<std::string> columnSet;
  for (const auto& field : fields) {
    columnSet.emplace(field.get_name());
  }
  if (fields.size() != columnSet.size()) {
    LOG(INFO) << "Conflict field in the tag index.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  // A maximum of 16 columns are allowed in the index.
  if (columnSet.size() > maxIndexLimit) {
    LOG(INFO) << "The number of index columns exceeds maximum limit " << maxIndexLimit;
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

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
    auto item = MetaKeyUtils::parseIndex(val);
    if (item.get_schema_id().getType() != nebula::cpp2::SchemaID::Type::tag_id ||
        fields.size() > item.get_fields().size() || tagID != item.get_schema_id().get_tag_id()) {
      checkIter->next();
      continue;
    }

    if (checkIndexExist(fields, item)) {
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
  std::vector<cpp2::ColumnDef> columns;
  for (auto& field : fields) {
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
    if (col.type.get_type() == nebula::cpp2::PropertyType::DURATION) {
      LOG(INFO) << "Field " << field.get_name() << " in Tag " << tagName << " is duration."
                << "It can not be indexed.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    }
    if (col.type.get_type() == nebula::cpp2::PropertyType::FIXED_STRING) {
      if (field.get_type_length() != nullptr) {
        LOG(INFO) << "Length should not be specified of fixed_string index :" << field.get_name();
        handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
      }
    } else if (col.type.get_type() == nebula::cpp2::PropertyType::STRING) {
      if (!field.type_length_ref().has_value()) {
        LOG(INFO) << "No type length set : " << field.get_name();
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
      }
      if (*field.get_type_length() > MAX_INDEX_TYPE_LENGTH) {
        LOG(INFO) << "Unsupported index type lengths greater than " << MAX_INDEX_TYPE_LENGTH
                  << " : " << field.get_name();
        handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
      }
      if (*field.get_type_length() <= 0) {
        LOG(INFO) << "Unsupported index type length <= 0 : " << field.get_name();
        handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
      }
      col.type.type_ref() = nebula::cpp2::PropertyType::FIXED_STRING;
      col.type.type_length_ref() = *field.get_type_length();
    } else if (field.type_length_ref().has_value()) {
      LOG(INFO) << "No need to set type length : " << field.get_name();
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
      onFinished();
      return;
    } else if (col.type.get_type() == nebula::cpp2::PropertyType::GEOGRAPHY && fields.size() > 1) {
      // TODO(jie): Support joint index for geography
      LOG(INFO) << "Only support to create index on a single geography column currently";
      handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
      onFinished();
      return;
    }
    columns.emplace_back(col);
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
  cpp2::IndexItem item;
  item.index_id_ref() = tagIndex;
  item.index_name_ref() = indexName;
  nebula::cpp2::SchemaID schemaID;
  schemaID.tag_id_ref() = tagID;
  item.schema_id_ref() = schemaID;
  item.schema_name_ref() = tagName;
  item.fields_ref() = std::move(columns);
  if (req.index_params_ref().has_value()) {
    item.index_params_ref() = *req.index_params_ref();
  }
  if (req.comment_ref().has_value()) {
    item.comment_ref() = *req.comment_ref();
  }

  data.emplace_back(MetaKeyUtils::indexIndexKey(space, indexName),
                    std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(IndexID)));
  data.emplace_back(MetaKeyUtils::indexKey(space, tagIndex), MetaKeyUtils::indexVal(item));
  LOG(INFO) << "Create Tag Index " << indexName << ", tagIndex " << tagIndex;
  resp_.id_ref() = to(tagIndex, EntryType::INDEX);
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
