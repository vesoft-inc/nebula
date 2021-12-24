/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/StorageClientCache.h"

using nebula::storage::cpp2::GetNeighborsRequest;
using nebula::storage::cpp2::GetNeighborsResponse;
using nebula::storage::cpp2::TraverseSpec;

namespace nebula {
namespace graph {

StorageClientCache::StorageClientCache() {
  cache_ = GraphCache::instance();
  // init colNames
  resultDataSet_.colNames.emplace_back(nebula::kVid);
  resultDataSet_.colNames.emplace_back("_stats");
}

std::string StorageClientCache::tagKey(const std::string& vId, TagID tagId) {
  std::string key;
  key.reserve(vId.size() + sizeof(TagID));
  key.append(vId.data(), vId.size()).append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
  return key;
}

std::string StorageClientCache::edgeKey(const std::string& srcVid, EdgeType type) {
  std::string key;
  key.reserve(srcVid.size() + sizeof(EdgeType));
  key.append(srcVid.data(), srcVid.size())
      .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
  return key;
}

Status StorageClientCache::checkCondition(const GetNeighborsRequest& req) {
  if (req.get_traverse_spec().filter_ref().has_value()) {
    return Status::Error("GetNeighbor contain filter expression");
  }
  if ((*req.traverse_spec_ref()).random_ref().has_value()) {
    return Status::Error("GetNeighbor contain random");
  }
  if ((*req.traverse_spec_ref()).random_ref().has_value()) {
    return Status::Error("GetNeighbor contain limit");
  }
  if (!req.get_traverse_spec().vertex_props_ref().has_value()) {
    return Status::Error("GetNeighbor contain vertexProps");
  }
  return Status::OK();
}

Status StorageClientCache::buildEdgeContext(const TraverseSpec& req) {
  if (!req.edge_props_ref().has_value()) {
    return Status::Error("GetNeighbor does not contain edgeProps");
  }
  auto edgeProps = *req.edge_props_ref();
  for (const auto& edgeProp : edgeProps) {
    auto edgeType = edgeProp.get_type();
    edgeTypes_.push_back(edgeType);
    // TODO getEdgeName throught edgeSchema
    std::string edgeName = "test";
    std::string colName = "_edge:";
    colName.append(edgeType > 0 ? "+" : "-").append(edgeName);
    if ((*edgeProp.props_ref()).empty()) {
      // should contain dst
      return Status::Error("%s 's prop is nullptr", edgeName.c_str());
    }
    for (const auto& name : (*edgeProp.props_ref())) {
      if (name != nebula::kDst) {
        // must contain dst
        return Status::Error("Edge : %s contain %s", edgeName.c_str(), name.c_str());
      }
    }
    // todo fix it
    colName += ":" + std::string(nebula::kDst);
    resultDataSet_.colNames.emplace_back(std::move(colName));
  }
  resultDataSet_.colNames.emplace_back("_expr");

  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
