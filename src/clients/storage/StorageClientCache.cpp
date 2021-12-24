/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/StorageClientCache.h"
// #include "graph/context/Result.h"
// #include "graph/context/Iterator.h"

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

template <class Request, class Response>
StatusOr<Response> StorageClientCache::getCacheValue(const Request& req) {
  if (!std::is_same<Request, GetNeighborsRequest>::value) {
    return Status::Error("Not GetNeighborsRequest");
  }
  NG_RETURN_IF_ERROR(checkCondition(req));

  NG_RETURN_IF_ERROR(buildEdgeContext(req.get_traverse_spec()));

  for (const auto& partEntry : req.get_parts()) {
    // auto partId = partEntry.first;
    for (const auto& entry : partEntry.second) {
      CHECK_GE(entry.values.size(), 1);
      auto vId = entry.values[0].getStr();
      std::vector<Value> row;
      /*
       * Format is
       * |_vid|_stats|_edge:like:_dst|_edge:-like:_dst|_edge:serve:_dst|_expr|
       * |"a" | NULL |[["b"],["c"]]  |[["d"],["e"]]   |[["f"], ["g"]]  | NULL|
       *
       * first column is vertexId, second column is reserved for stats
       */
      row.emplace_back(vId);
      row.emplace_back(Value());

      for (const auto edgeType : edgeTypes_) {
        const auto& eKey = edgeKey(vId, edgeType);
        auto status = cache_->getEdges(eKey);
        // if onekey cache miss return
        NG_RETURN_IF_ERROR(status);
        auto dstRes = status.value();

        nebula::List dstList;
        dstList.values.reserve(dstRes.size());
        for (const auto& dst : dstRes) {
          nebula::List propList;
          propList.values.emplace_back(std::move(dst));
          dstList.values.emplace_back(std::move(propList));
        }
        nebula::List cell;
        cell.values.emplace_back(std::move(dstList));
        row.emplace_back(std::move(cell));
      }
      // col : expr
      row.emplace_back(Value());
      resultDataSet_.rows.emplace_back(std::move(row));
    }
  }
  Response resp;
  resp.vertices_ref() = std::move(resultDataSet_);
  return resp;
}

void StorageClientCache::insertResultIntoCache(GetNeighborsResponse& resp) {
  auto dataset = resp.get_vertices();
  if (dataset == nullptr) {
    LOG(INFO) << "GraphCache Empty dataset in response";
    return;
  }
  //  list.values.emplace_back(std::move(*dataset));
  //  ResultBuilder builder;
  //  builder.state(Result::State::kSuccess);
  //  builder.value(Value(std::move(list))).iter(Iterator::Kind::kGetNeighbors);

  auto colSize = dataset->colNames.size();
  if (edgeTypes_.size() + 3 != colSize) {
    LOG(INFO) << "RPC Dataset colsize error";
    return;
  }

  auto& rows = (*dataset).rows;
  for (const auto& row : rows) {
    // first column is vertexId
    auto vId = row.values[0].getStr();
    for (size_t i = 2; i < row.values.size() - 1; ++i) {
      auto edgeType = edgeTypes_[i - 2];
      auto& cell = row.values[i];
      // DCHECK(nebula::List, cell);
      auto& cellList = cell.getList();
      std::vector<std::string> edgeDsts;
      edgeDsts.reserve(cellList.size());
      for (const auto& dst : cellList.values) {
        edgeDsts.emplace_back(dst.getStr());
      }
      if (!cache_->addAllEdges(edgeKey(vId, edgeType), edgeDsts)) {
        LOG(INFO) << "Cache vid : " << vId << " edgeType: " << edgeType << " fail.";
      }
    }
  }
  LOG(INFO) << "Insert Result Into Cache Success";
}

}  // namespace graph
}  // namespace nebula
