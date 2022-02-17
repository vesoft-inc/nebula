/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/mutate/InsertExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>, Try::Tr...
#include <folly/futures/Future.h>   // for Future::Future<T>
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException::Pro...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException::Pro...

#include <algorithm>      // for max
#include <string>         // for string, basic_string
#include <unordered_map>  // for operator!=
#include <utility>        // for move
#include <vector>         // for vector

#include "clients/storage/StorageClient.h"      // for StorageClient, Storag...
#include "clients/storage/StorageClientBase.h"  // for StorageRpcResponse
#include "common/base/Logging.h"                // for COMPACT_GOOGLE_LOG_INFO
#include "common/base/Status.h"                 // for Status, NG_RETURN_IF_...
#include "common/base/StatusOr.h"               // for StatusOr
#include "common/thrift/ThriftTypes.h"          // for TagID
#include "common/time/Duration.h"               // for Duration
#include "common/time/ScopedTimer.h"            // for SCOPED_TIMER
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/planner/plan/ExecutionPlan.h"   // for ExecutionPlan
#include "graph/planner/plan/Mutate.h"          // for InsertEdges, InsertVe...
#include "graph/service/GraphFlags.h"           // for FLAGS_enable_experime...
#include "graph/service/RequestContext.h"       // for RequestContext
#include "graph/session/ClientSession.h"        // for ClientSession
#include "interface/gen-cpp2/storage_types.h"   // for NewEdge, NewVertex

using nebula::storage::StorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> InsertVerticesExecutor::execute() {
  return insertVertices();
}

folly::Future<Status> InsertVerticesExecutor::insertVertices() {
  SCOPED_TIMER(&execTime_);

  auto *ivNode = asNode<InsertVertices>(node());
  time::Duration addVertTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      ivNode->getSpace(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  return qctx()
      ->getStorageClient()
      ->addVertices(param,
                    ivNode->getVertices(),
                    ivNode->getPropNames(),
                    ivNode->getIfNotExists(),
                    ivNode->getIgnoreExistedIndex())
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

folly::Future<Status> InsertEdgesExecutor::execute() {
  return insertEdges();
}

folly::Future<Status> InsertEdgesExecutor::insertEdges() {
  SCOPED_TIMER(&execTime_);

  auto *ieNode = asNode<InsertEdges>(node());
  time::Duration addEdgeTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      ieNode->getSpace(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = FLAGS_enable_experimental_feature;
  return qctx()
      ->getStorageClient()
      ->addEdges(param,
                 ieNode->getEdges(),
                 ieNode->getPropNames(),
                 ieNode->getIfNotExists(),
                 ieNode->getIgnoreExistedIndex())
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
