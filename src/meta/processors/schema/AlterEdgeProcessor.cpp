/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/AlterEdgeProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <stdint.h>                         // for int64_t
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>  // for max
#include <memory>     // for unique_ptr
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for operator<<, char_traits
#include <vector>     // for vector

#include "common/base/ErrorOr.h"                // for error, ok, value
#include "common/base/Logging.h"                // for LOG, LogMessage, _LOG...
#include "common/thrift/ThriftTypes.h"          // for GraphSpaceID
#include "common/utils/MetaKeyUtils.h"          // for MetaKeyUtils, EntryType
#include "interface/gen-cpp2/common_types.h"    // for ErrorCode, ErrorCode:...
#include "kvstore/Common.h"                     // for KV
#include "kvstore/KVIterator.h"                 // for KVIterator
#include "meta/MetaServiceUtils.h"              // for MetaServiceUtils
#include "meta/processors/BaseProcessor.h"      // for BaseProcessor::doPrefix
#include "meta/processors/Common.h"             // for LockUtils
#include "meta/processors/schema/SchemaUtil.h"  // for SchemaUtil

namespace nebula {
namespace meta {

void AlterEdgeProcessor::process(const cpp2::AlterEdgeReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& edgeName = req.get_edge_name();

  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder wHolder(LockUtils::tagAndEdgeLock());
  auto ret = getEdgeType(spaceId, edgeName);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "Failed to get edge " << edgeName << " error "
               << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto edgeType = nebula::value(ret);

  // Check the edge belongs to the space
  auto edgePrefix = MetaKeyUtils::schemaEdgePrefix(spaceId, edgeType);
  auto retPre = doPrefix(edgePrefix);
  if (!nebula::ok(retPre)) {
    auto retCode = nebula::error(retPre);
    LOG(ERROR) << "Edge Prefix failed, edgename: " << edgeName << ", spaceId " << spaceId
               << " error " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto iter = nebula::value(retPre).get();
  if (!iter->valid()) {
    LOG(ERROR) << "Edge could not be found, spaceId " << spaceId << ", edgename: " << edgeName;
    handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
    onFinished();
    return;
  }

  // Get lasted version of edge
  auto version = MetaKeyUtils::parseEdgeVersion(iter->key()) + 1;
  auto schema = MetaKeyUtils::parseSchema(iter->val());
  auto columns = schema.get_columns();
  auto prop = schema.get_schema_prop();

  // Update schema column
  auto& edgeItems = req.get_edge_items();

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
      LOG(ERROR) << "Alter edge error, index conflict : "
                 << apache::thrift::util::enumNameSafe(iStatus);
      handleErrorCode(iStatus);
      onFinished();
      return;
    }
  }

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
      LOG(ERROR) << "Alter edge error, index and ttl conflict";
      handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
      onFinished();
      return;
    }
  }

  // check fulltext index
  auto ftIdxRet = getFTIndex(spaceId, edgeType);
  if (nebula::ok(ftIdxRet)) {
    auto fti = std::move(nebula::value(ftIdxRet));
    auto ftStatus = ftIndexCheck(fti.get_fields(), edgeItems);
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
      auto retCode =
          MetaServiceUtils::alterColumnDefs(columns, prop, col, *edgeItem.op_ref(), true);
      if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Alter edge column error " << apache::thrift::util::enumNameSafe(retCode);
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
    LOG(ERROR) << "Alter edge property error " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  schema.schema_prop_ref() = std::move(prop);
  schema.columns_ref() = std::move(columns);

  std::vector<kvstore::KV> data;
  LOG(INFO) << "Alter edge " << edgeName << ", edgeType " << edgeType;
  data.emplace_back(MetaKeyUtils::schemaEdgeKey(spaceId, edgeType, version),
                    MetaKeyUtils::schemaVal(edgeName, schema));
  resp_.id_ref() = to(edgeType, EntryType::EDGE);
  doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
