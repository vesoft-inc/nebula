/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/GetEdgeIndexProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <string>  // for operator<<, char_traits

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void GetEdgeIndexProcessor::process(const cpp2::GetEdgeIndexReq& req) {
  auto spaceID = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceID);
  auto indexName = req.get_index_name();
  folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeIndexLock());
  auto edgeIndexIDRet = getIndexID(spaceID, indexName);
  if (!nebula::ok(edgeIndexIDRet)) {
    auto retCode = nebula::error(edgeIndexIDRet);
    LOG(ERROR) << "Get Edge Index SpaceID: " << spaceID << " Index Name: " << indexName
               << " failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto indexId = nebula::value(edgeIndexIDRet);
  LOG(INFO) << "Get Edge Index SpaceID: " << spaceID << " Index Name: " << indexName;
  const auto& indexKey = MetaKeyUtils::indexKey(spaceID, indexId);
  auto indexItemRet = doGet(indexKey);
  if (!nebula::ok(indexItemRet)) {
    auto retCode = nebula::error(indexItemRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    LOG(ERROR) << "Get Edge Index Failed: SpaceID " << spaceID << " Index Name: " << indexName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto item = MetaKeyUtils::parseIndex(nebula::value(indexItemRet));
  if (item.get_schema_id().getType() != nebula::cpp2::SchemaID::Type::edge_type) {
    LOG(ERROR) << "Get Edge Index Failed: Index Name " << indexName << " is not EdgeIndex";
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    onFinished();
    return;
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.item_ref() = std::move(item);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
