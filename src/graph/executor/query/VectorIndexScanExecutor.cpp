// Copyright (c) 2024 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/VectorIndexScanExecutor.h"

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/Constants.h"
#include "graph/util/VectorIndexUtils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula::graph {

folly::Future<Status> VectorIndexScanExecutor::execute() {
  auto esAdapterResult = VectorIndexUtils::getESAdapter(qctx_->getMetaClient());
  if (!esAdapterResult.ok()) {
    return esAdapterResult.status();
  }
  esAdapter_ = std::move(esAdapterResult).value();
  auto* vectorIndexScan = asNode<VectorIndexScan>(node());
  auto esQueryResult = accessVectorIndex(vectorIndexScan->searchExpression());
  if (!esQueryResult.ok()) {
    LOG(ERROR) << esQueryResult.status().message();
    return esQueryResult.status();
  }
  auto esResultValue = std::move(esQueryResult).value();
  const auto& space = qctx()->rctx()->session()->space();
  if (!isIntVidType(space)) {
    if (vectorIndexScan->isEdge()) {
      DataSet edges({"id", kScore});
      for (auto& item : esResultValue.items) {
        Edge edge;
        edge.src = item.src;
        edge.dst = item.dst;
        edge.ranking = item.rank;
        edge.type = vectorIndexScan->schemaId();
        edges.emplace_back(Row({std::move(edge), item.score}));
      }
      finish(ResultBuilder().value(Value(std::move(edges))).iter(Iterator::Kind::kProp).build());
    } else {
      DataSet vertices({"id", kScore});
      for (auto& item : esResultValue.items) {
        vertices.emplace_back(Row({item.vid, item.score}));
      }
      finish(ResultBuilder().value(Value(std::move(vertices))).iter(Iterator::Kind::kProp).build());
    }
  } else {
    if (vectorIndexScan->isEdge()) {
      DataSet edges({"id", kScore});
      for (auto& item : esResultValue.items) {
        Edge edge;
        edge.src = std::stol(item.src);
        edge.dst = std::stol(item.dst);
        edge.ranking = item.rank;
        edge.type = vectorIndexScan->schemaId();
        edges.emplace_back(Row({std::move(edge), item.score}));
      }
      finish(ResultBuilder().value(Value(std::move(edges))).iter(Iterator::Kind::kProp).build());
    } else {
      DataSet vertices({"id", kScore});
      for (auto& item : esResultValue.items) {
        std::string vidStr = item.vid;
        int64_t vid = std::stol(vidStr);
        vertices.emplace_back(Row({vid, item.score}));
      }
      finish(ResultBuilder().value(Value(std::move(vertices))).iter(Iterator::Kind::kProp).build());
    }
  }
  return Status::OK();
}

StatusOr<plugin::ESQueryResult> VectorIndexScanExecutor::accessVectorIndex(
    VectorQueryExpression* expr) {
  std::function<StatusOr<plugin::ESQueryResult>()> execFunc;
  plugin::ESAdapter& esAdapter = esAdapter_;
  auto* vectorIndexScan = asNode<VectorIndexScan>(node());
  switch (expr->kind()) {
    case Expression::Kind::kVectorQuery: {
      auto arg = expr->arg();
      auto index = arg->index();
      auto field = arg->field();
      int64_t topk = vectorIndexScan->getValidLimit();
      if (topk == 0) {
        return plugin::ESQueryResult();
      }
      execFunc = [=, &esAdapter]() {
        return esAdapter.vectorQuery(index, field, topk);
      };
      break;
    }
    default: {
      return Status::SemanticError("vector search expression error");
    }
  }
  return execFunc();
}

}  // namespace nebula::graph 