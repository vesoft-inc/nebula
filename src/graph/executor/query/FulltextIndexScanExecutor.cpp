// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/FulltextIndexScanExecutor.h"

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/FTIndexUtils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula::graph {

folly::Future<Status> FulltextIndexScanExecutor::execute() {
  auto esAdapterResult = FTIndexUtils::getESAdapter(qctx_->getMetaClient());
  if (!esAdapterResult.ok()) {
    return esAdapterResult.status();
  }
  esAdapter_ = std::move(esAdapterResult).value();
  auto* ftIndexScan = asNode<FulltextIndexScan>(node());
  auto esQueryResult = accessFulltextIndex(ftIndexScan->searchExpression());
  if (!esQueryResult.ok()) {
    LOG(ERROR) << esQueryResult.status().message();
    return esQueryResult.status();
  }
  auto esResultValue = std::move(esQueryResult).value();
  const auto& space = qctx()->rctx()->session()->space();
  if (!isIntVidType(space)) {
    if (ftIndexScan->isEdge()) {
      DataSet edges({"edge"});
      for (auto& item : esResultValue.items) {
        // TODO(hs.zhang): return item.score
        Edge edge;
        edge.src = item.src;
        edge.dst = item.dst;
        edge.ranking = item.rank;
        edge.type = ftIndexScan->schemaId();
        edges.emplace_back(Row({std::move(edge)}));
      }
      finish(ResultBuilder().value(Value(std::move(edges))).iter(Iterator::Kind::kProp).build());
    } else {
      DataSet vertices({kVid});
      for (auto& item : esResultValue.items) {
        // TODO(hs.zhang): return item.score
        vertices.emplace_back(Row({item.vid}));
      }
      finish(ResultBuilder().value(Value(std::move(vertices))).iter(Iterator::Kind::kProp).build());
    }
  } else {
    if (ftIndexScan->isEdge()) {
      DataSet edges({kSrc, kRank, kDst});
      for (auto& item : esResultValue.items) {
        std::string srcStr = item.src;
        std::string dstStr = item.dst;
        int64_t src = *reinterpret_cast<int64_t*>(srcStr.data());
        int64_t dst = *reinterpret_cast<int64_t*>(dstStr.data());
        edges.emplace_back(Row({src, item.rank, dst}));
      }
      finish(ResultBuilder().value(Value(std::move(edges))).iter(Iterator::Kind::kProp).build());
    } else {
      DataSet vertices({kVid});
      for (auto& item : esResultValue.items) {
        std::string vidStr = item.vid;
        int64_t vid = *reinterpret_cast<int64_t*>(vidStr.data());
        vertices.emplace_back(Row({vid}));
      }
      finish(ResultBuilder().value(Value(std::move(vertices))).iter(Iterator::Kind::kProp).build());
    }
  }
  return Status::OK();
}

StatusOr<plugin::ESQueryResult> FulltextIndexScanExecutor::accessFulltextIndex(
    TextSearchExpression* tsExpr) {
  std::function<StatusOr<nebula::plugin::ESQueryResult>()> execFunc;
  plugin::ESAdapter& esAdapter = esAdapter_;
  switch (tsExpr->kind()) {
    case Expression::Kind::kESQUERY: {
      auto arg = tsExpr->arg();
      auto index = arg->index();
      auto query = arg->query();
      auto props = arg->props();
      auto count = arg->count();
      auto offset = arg->offset();
      execFunc = [=, &esAdapter]() {
        return esAdapter.queryString(index, query, props, offset, count);
      };
      break;
    }
    default: {
      return Status::SemanticError("text search expression error");
    }
  }

  return execFunc();
}

}  // namespace nebula::graph
