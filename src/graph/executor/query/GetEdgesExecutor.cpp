// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/GetEdgesExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
  return getEdges();
}

StatusOr < std::vector<std::vector<Value>> GetEdgesExecutor::buildRequestEdges(const GetEdges *ge) {
  auto valueIter = ectx_->getResult(ge->inputVar()).iter();
  QueryExpressionContext exprCtx(qctx()->ectx());

  std::vector<std::vector<Value>> edges;
  edges.reserve(valueIter->size());
  std::unordered_set<std::tuple<Value, Value, Value, Value>> uniqueEdges;
  uniqueEdges.reserve(valueIter->size());

  const auto &space = qctx()->rctx()->session()->space();
  const auto &metaVidType = *(space.spaceDesc.vid_type_ref());
  auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
  for (; valueIter->valid(); valueIter->next()) {
    auto type = ge->type()->eval(exprCtx(valueIter.get()));
    const auto &src = ge->src()->eval(exprCtx(valueIter.get()));
    const auto &dst = ge->dst()->eval(exprCtx(valueIter.get()));
    auto rank = ge->ranking()->eval(exprCtx(valueIter.get()));
    if (src.type() != vidType) {
      std::stringstream ss;
      ss << "`" << src.toString() << "', the src should be type of " << vidType << ", but was`"
         << src.type() << "'";
      return Status::Error(ss.str());
    }
    if (dst.type() != vidType) {
      std::stringstream ss;
      ss << "`" << dst.toString() << "', the dst should be type of " << vidType << ", but was`"
         << dst.type() << "'";
      return Status::Error(ss.str());
    }
    type = type < 0 ? -type : type;
    auto edgeKey = std::make_tuple(src, type, rank, dst);
    if (ge->dedup() && !uniqueEdges.emplace(std::move(edgeKey)).second) {
      continue;
    }
    if (!rank.isInt()) {
      continue;
    }
    edges.emplace_back(std::vector<Value>({src, type, rank, dst}));
  }
  return edges;
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
  SCOPED_TIMER(&execTime_);
  StorageClient *client = qctx()->getStorageClient();
  auto *ge = asNode<GetEdges>(node());
  if (ge->src() == nullptr || ge->type() == nullptr || ge->ranking() == nullptr ||
      ge->dst() == nullptr) {
    return Status::Error("ptr is nullptr");
  }

  auto res = buildRequestEdges(ge);
  NG_RETURN_IF_ERROR(res);
  auto edges = std::move(res).value();

  if (edges.empty()) {
    // TODO: add test for empty input.
    return finish(
        ResultBuilder().value(Value(DataSet(ge->colNames()))).iter(Iterator::Kind::kProp).build());
  }

  time::Duration getPropsTime;
  StorageClient::CommonRequestParam param(ge->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(client)
      ->getEdgeProps(param,
                     std::move(edges),
                     ge->props(),
                     ge->exprs(),
                     ge->dedup(),
                     ge->orderBy(),
                     ge->limit(qctx()),
                     ge->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this, ge](StorageRpcResponse<GetPropResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), ge->colNames());
      });
}

}  // namespace graph
}  // namespace nebula
