/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/CreateEdgeProcessor.h"

#include "meta/processors/schema/SchemaUtil.h"

namespace nebula {
namespace meta {

void CreateEdgeProcessor::process(const cpp2::CreateEdgeReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& edgeName = req.get_edge_name();
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  // Check if the tag with same name exists
  auto conflictRet = getTagId(spaceId, edgeName);
  if (nebula::ok(conflictRet)) {
    LOG(INFO) << "Failed to create edge `" << edgeName
              << "': some tag with the same name already exists.";
    resp_.id_ref() = to(nebula::value(conflictRet), EntryType::EDGE);
    handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
    onFinished();
    return;
  } else {
    auto retCode = nebula::error(conflictRet);
    if (retCode != nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND) {
      LOG(INFO) << "Failed to create edge " << edgeName << " error "
                << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
  }

  auto columns = req.get_schema().get_columns();
  if (!SchemaUtil::checkType(columns)) {
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  cpp2::Schema schema;
  schema.columns_ref() = std::move(columns);
  schema.schema_prop_ref() = req.get_schema().get_schema_prop();

  auto ret = getEdgeType(spaceId, edgeName);
  if (nebula::ok(ret)) {
    if (req.get_if_not_exists()) {
      handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    } else {
      LOG(INFO) << "Create Edge Failed :" << edgeName << " has existed";
      handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
    }
    resp_.id_ref() = to(nebula::value(ret), EntryType::EDGE);
    onFinished();
    return;
  } else {
    auto retCode = nebula::error(ret);
    if (retCode != nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND) {
      LOG(INFO) << "Failed to create edge " << edgeName << " error "
                << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
  }

  auto edgeTypeRet = autoIncrementIdInSpace(spaceId);
  if (!nebula::ok(edgeTypeRet)) {
    LOG(INFO) << "Create edge failed : Get edge type id failed";
    handleErrorCode(nebula::error(edgeTypeRet));
    onFinished();
    return;
  }

  auto edgeType = nebula::value(edgeTypeRet);
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::indexEdgeKey(spaceId, edgeName),
                    std::string(reinterpret_cast<const char*>(&edgeType), sizeof(EdgeType)));
  data.emplace_back(MetaKeyUtils::schemaEdgeKey(spaceId, edgeType, 0),
                    MetaKeyUtils::schemaVal(edgeName, schema));

  LOG(INFO) << "Create Edge " << edgeName << ", edgeType " << edgeType;
  resp_.id_ref() = to(edgeType, EntryType::EDGE);
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
