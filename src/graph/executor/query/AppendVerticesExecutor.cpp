/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/query/AppendVerticesExecutor.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {
folly::Future<Status> AppendVerticesExecutor::execute() { return appendVertices(); }

DataSet AppendVerticesExecutor::buildRequestDataSet(const AppendVertices *av) {
  if (av == nullptr) {
    return nebula::DataSet({kVid});
  }
  auto valueIter = ectx_->getResult(av->inputVar()).iter();
  VLOG(1) << "src expr: " << av->src()->toString();
  return buildRequestDataSetByVidType(valueIter.get(), av->src(), av->dedup());
}

folly::Future<Status> AppendVerticesExecutor::appendVertices() {
  SCOPED_TIMER(&execTime_);

  auto *av = asNode<AppendVertices>(node());
  GraphStorageClient *storageClient = qctx()->getStorageClient();

  DataSet vertices = buildRequestDataSet(av);
  if (vertices.rows.empty()) {
    return finish(ResultBuilder().value(Value(DataSet(av->colNames()))).build());
  }

  time::Duration getPropsTime;
  return DCHECK_NOTNULL(storageClient)
      ->getProps(av->space(),
                 qctx()->rctx()->session()->id(),
                 qctx()->plan()->id(),
                 std::move(vertices),
                 av->props(),
                 nullptr,
                 av->exprs(),
                 av->dedup(),
                 av->orderBy(),
                 av->limit(),
                 av->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this](StorageRpcResponse<GetPropResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp));
      });
}

Status AppendVerticesExecutor::handleResp(
    storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp) {
  auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto state = std::move(result).value();
  std::unordered_map<Value, Value> map;
  for (auto &resp : rpcResp.responses()) {
    if (resp.props_ref().has_value()) {
      auto iter = PropIter(std::make_shared<Value>(std::move(*resp.props_ref())));
      for (; iter.valid(); iter.next()) {
        map.emplace(iter.getColumn(kVid), iter.getVertex());
      }
    }
  }

  auto *av = asNode<GetVertices>(node());
  auto iter = qctx()->ectx()->getResult(av->inputVar()).iter();
  auto src = av->src();
  QueryExpressionContext ctx(qctx()->ectx());

  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    VLOG(1) << "dst: " << src->eval(ctx(iter.get()));
    auto dstFound = map.find(src->eval(ctx(iter.get())));
    auto row = static_cast<SequentialIter *>(iter.get())->moveRow();
    if (dstFound == map.end()) {
      return Status::Error("Dst not faound.");
    }
    row.values.emplace_back(dstFound->second);
    ds.rows.emplace_back(std::move(row));
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).state(state).build());
}

}  // namespace graph
}  // namespace nebula
