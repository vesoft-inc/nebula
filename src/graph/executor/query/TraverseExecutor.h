/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
#define EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_

#include <vector>

#include "clients/storage/GraphStorageClient.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"
#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace graph {

using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;

class TraverseExecutor final : public StorageAccessExecutor {
 public:
  TraverseExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("Traverse", node, qctx) {
    traverse_ = asNode<Traverse>(node);
  }

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  Status buildRequestDataSet();

  folly::Future<Status> traverse();

  void addStats(RpcResponse& resps);

  void getNeighbors();

  void handleResponse(RpcResponse& resps);

  Status buildInterimPath(GetNeighborsIter* iter);

  Status buildResult();

  bool isFinalStep() const { return currentStep_ == steps_.nSteps(); }

 private:
  DataSet reqDs_;
  const Traverse* traverse_{nullptr};
  folly::Promise<Status> promise_;
  StepClause steps_;
  size_t currentStep_{0};
  using Dst = Value;
  using Paths = std::vector<Row>;
  std::list<std::unordered_map<Value, Paths>> paths_;
};

}  // namespace graph
}  // namespace nebula

#endif  // EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
