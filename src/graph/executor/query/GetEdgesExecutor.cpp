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

StatusOr<DataSet> GetEdgesExecutor::buildRequestDataSet(const GetEdges *ge) {
  auto valueIter = ectx_->getResult(ge->inputVar()).iter();
  QueryExpressionContext exprCtx(qctx()->ectx());

  nebula::DataSet edges({kSrc, kType, kRank, kDst});
  edges.rows.reserve(valueIter->size());
  std::unordered_set<std::tuple<Value, Value, Value, Value>> uniqueEdges;
  uniqueEdges.reserve(valueIter->size());

  const auto &space = qctx()->rctx()->session()->space();
  const auto &vidType = *(space.spaceDesc.vid_type_ref());
  for (; valueIter->valid(); valueIter->next()) {
    auto type = ge->type()->eval(exprCtx(valueIter.get()));
    auto src = ge->src()->eval(exprCtx(valueIter.get()));
    auto dst = ge->dst()->eval(exprCtx(valueIter.get()));
    auto rank = ge->ranking()->eval(exprCtx(valueIter.get()));
    if (!SchemaUtil::isValidVid(src, vidType)) {
      std::stringstream ss;
      ss << "`" << src.toString() << "', the src should be type of "
         << apache::thrift::util::enumNameSafe(vidType.get_type()) << ", but was`" << src.type()
         << "'";
      return Status::Error(ss.str());
    }
    if (!SchemaUtil::isValidVid(dst, vidType)) {
      std::stringstream ss;
      ss << "`" << dst.toString() << "', the dst should be type of "
         << apache::thrift::util::enumNameSafe(vidType.get_type()) << ", but was`" << dst.type()
         << "'";
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
    edges.emplace_back(Row({std::move(src), type, rank, std::move(dst)}));
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

  auto res = buildRequestDataSet(ge);
  NG_RETURN_IF_ERROR(res);
  auto edges = std::move(res).value();

  if (edges.rows.empty()) {
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
      ->getProps(param,
                 std::move(edges),
                 nullptr,
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
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), ge->colNames());
      });
}

}  // namespace graph
}  // namespace nebula
