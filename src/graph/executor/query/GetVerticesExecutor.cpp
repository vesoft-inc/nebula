// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/GetVerticesExecutor.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetVerticesExecutor::execute() {
  return getVertices();
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
  SCOPED_TIMER(&execTime_);

  auto *gv = asNode<GetVertices>(node());
  StorageClient *storageClient = qctx()->getStorageClient();

  auto res = buildRequestDataSet(gv);
  NG_RETURN_IF_ERROR(res);
  auto vertices = std::move(res).value();
  if (vertices.rows.empty()) {
    // TODO: add test for empty input.
    return finish(
        ResultBuilder().value(Value(DataSet(gv->colNames()))).iter(Iterator::Kind::kProp).build());
  }

  time::Duration getPropsTime;
  StorageClient::CommonRequestParam param(gv->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  auto limit = gv->limit();
  if (limit < 0) {
    limit = std::numeric_limits<int32_t>::max();
  }
  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 gv->props(),
                 nullptr,
                 gv->exprs(),
                 gv->dedup(),
                 gv->orderBy(),
                 limit,
                 gv->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        addState("total_rpc", getPropsTime);
      })
      .thenValue([this, gv](StorageRpcResponse<GetPropResponse> &&rpcResp) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp);
        return handleResp(std::move(rpcResp), gv->colNames());
      });
}

StatusOr<DataSet> GetVerticesExecutor::buildRequestDataSet(const GetVertices *gv) {
  if (gv == nullptr) {
    return nebula::DataSet({kVid});
  }
  // Accept Table such as | $a | $b | $c |... as input which one column indicate
  // src
  auto valueIter = ectx_->getResult(gv->inputVar()).iter();
  return buildRequestDataSetByVidType(valueIter.get(), gv->src(), gv->dedup());
}

}  // namespace graph
}  // namespace nebula
