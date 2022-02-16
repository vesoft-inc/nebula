/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/ListEdgesProcessor.h"

#include <folly/Range.h>               // for Range
#include <folly/SharedMutex.h>         // for SharedMutex
#include <stdint.h>                    // for int32_t
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>  // for max
#include <memory>     // for unique_ptr
#include <ostream>    // for basic_ostream::operat...
#include <string>     // for basic_string
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/thrift/ThriftTypes.h"        // for EdgeType, GraphSpaceID
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doPrefix
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void ListEdgesProcessor::process(const cpp2::ListEdgesReq &req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  folly::SharedMutex::ReadHolder rHolder(LockUtils::tagAndEdgeLock());
  auto prefix = MetaKeyUtils::schemaEdgesPrefix(spaceId);
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    LOG(ERROR) << "List Edges failed, SpaceID: " << spaceId;
    handleErrorCode(nebula::error(ret));
    onFinished();
    return;
  }

  auto iter = nebula::value(ret).get();
  std::vector<nebula::meta::cpp2::EdgeItem> edges;
  while (iter->valid()) {
    auto key = iter->key();
    auto val = iter->val();
    auto edgeType = *reinterpret_cast<const EdgeType *>(key.data() + prefix.size());
    auto version = MetaKeyUtils::parseEdgeVersion(key);
    auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto edgeName = val.subpiece(sizeof(int32_t), nameLen).str();
    auto schema = MetaKeyUtils::parseSchema(val);
    cpp2::EdgeItem edge;
    edge.edge_type_ref() = edgeType;
    edge.edge_name_ref() = std::move(edgeName);
    edge.version_ref() = version;
    edge.schema_ref() = std::move(schema);
    edges.emplace_back(std::move(edge));
    iter->next();
  }
  resp_.edges_ref() = std::move(edges);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
