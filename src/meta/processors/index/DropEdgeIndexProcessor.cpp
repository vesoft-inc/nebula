/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/DropEdgeIndexProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>  // for max
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for string, operator<<
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils, EntryType
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void DropEdgeIndexProcessor::process(const cpp2::DropEdgeIndexReq& req) {
  auto spaceID = req.get_space_id();
  const auto& indexName = req.get_index_name();
  CHECK_SPACE_ID_AND_RETURN(spaceID);
  folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());

  auto edgeIndexIDRet = getIndexID(spaceID, indexName);
  if (!nebula::ok(edgeIndexIDRet)) {
    auto retCode = nebula::error(edgeIndexIDRet);
    if (retCode == nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(ERROR) << "Drop Edge Index Failed, index name " << indexName
                   << " not exists in Space: " << spaceID;
      }
    } else {
      LOG(ERROR) << "Drop Edge Index Failed, index name " << indexName
                 << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto edgeIndexID = nebula::value(edgeIndexIDRet);

  std::vector<std::string> keys;
  keys.emplace_back(MetaKeyUtils::indexIndexKey(spaceID, indexName));
  keys.emplace_back(MetaKeyUtils::indexKey(spaceID, edgeIndexID));

  auto indexItemRet = doGet(keys.back());
  if (!nebula::ok(indexItemRet)) {
    auto retCode = nebula::error(indexItemRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    LOG(ERROR) << "Drop Edge Index Failed: SpaceID " << spaceID << " Index Name: " << indexName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto item = MetaKeyUtils::parseIndex(nebula::value(indexItemRet));
  if (item.get_schema_id().getType() != nebula::cpp2::SchemaID::Type::edge_type) {
    LOG(ERROR) << "Drop Edge Index Failed: Index Name " << indexName << " is not EdgeIndex";
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    onFinished();
    return;
  }

  LOG(INFO) << "Drop Edge Index " << indexName;
  resp_.id_ref() = to(edgeIndexID, EntryType::INDEX);
  doSyncMultiRemoveAndUpdate(std::move(keys));
}

}  // namespace meta
}  // namespace nebula
