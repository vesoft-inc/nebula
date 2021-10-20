/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/mutate/InsertExecutor.h"

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Mutate.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/ScopedTimer.h"

using nebula::storage::GraphStorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> InsertVerticesExecutor::execute() { return insertVertices(); }

folly::Future<Status> InsertVerticesExecutor::insertVertices() {
  SCOPED_TIMER(&execTime_);

  auto *ivNode = asNode<InsertVertices>(node());
  time::Duration addVertTime;
  auto plan = qctx()->plan();
  GraphStorageClient::CommonRequestParam param(
      ivNode->getSpace(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  return qctx()
      ->getStorageClient()
      ->addVertices(param, ivNode->getVertices(), ivNode->getPropNames(), ivNode->getIfNotExists())
      .via(runner())
      .ensure([addVertTime]() {
        VLOG(1) << "Add vertices time: " << addVertTime.elapsedInUSec() << "us";
      })
      .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
        return Status::OK();
      });
}

folly::Future<Status> InsertEdgesExecutor::execute() { return insertEdges(); }

folly::Future<Status> InsertEdgesExecutor::insertEdges() {
  SCOPED_TIMER(&execTime_);

  auto *ieNode = asNode<InsertEdges>(node());
  time::Duration addEdgeTime;
  auto plan = qctx()->plan();
  GraphStorageClient::CommonRequestParam param(
      ieNode->getSpace(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = FLAGS_enable_experimental_feature;
  return qctx()
      ->getStorageClient()
      ->addEdges(param, ieNode->getEdges(), ieNode->getPropNames(), ieNode->getIfNotExists())
      .via(runner())
      .ensure(
          [addEdgeTime]() { VLOG(1) << "Add edge time: " << addEdgeTime.elapsedInUSec() << "us"; })
      .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
        return Status::OK();
      });
}
}  // namespace graph
}  // namespace nebula
