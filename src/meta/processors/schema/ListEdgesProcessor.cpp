/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/ListEdgesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgesProcessor::process(const cpp2::ListEdgesReq &req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  // The EdgeItem obtained directly through edgeType is inaccurate (it may be invalid data).
  // It is accurate to obtain edgeType by traversing the edge index table, and then obtain EdgeItem
  // through edgeType
  auto indexEdgePrefix = MetaKeyUtils::indexEdgePrefix(spaceId);
  auto indexEdgeRet = doPrefix(indexEdgePrefix);
  if (!nebula::ok(indexEdgeRet)) {
    LOG(INFO) << "List Edges failed, SpaceID: " << spaceId;
    handleErrorCode(nebula::error(indexEdgeRet));
    onFinished();
    return;
  }

  auto indexEdgeIter = nebula::value(indexEdgeRet).get();
  std::vector<nebula::meta::cpp2::EdgeItem> edges;

  while (indexEdgeIter->valid()) {
    auto edgeType = *reinterpret_cast<const EdgeType *>(indexEdgeIter->val().toString().c_str());

    auto edgeSchemaPrefix = MetaKeyUtils::schemaEdgePrefix(spaceId, edgeType);
    auto edgeSchemaRet = doPrefix(edgeSchemaPrefix);
    if (!nebula::ok(edgeSchemaRet)) {
      LOG(INFO) << "Get Edge failed, SpaceID: " << spaceId << ", edgeType: " << edgeType;
      handleErrorCode(nebula::error(edgeSchemaRet));
      onFinished();
      return;
    }

    auto edgeSchemaIter = nebula::value(edgeSchemaRet).get();
    while (edgeSchemaIter->valid()) {
      auto key = edgeSchemaIter->key();
      auto val = edgeSchemaIter->val();
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
      edgeSchemaIter->next();
    }
    indexEdgeIter->next();
  }

  resp_.edges_ref() = std::move(edges);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
