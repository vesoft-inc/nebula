// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
#define EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"
// Subgraph receive result from GetNeighbors
// There are two Main functions
// First : Extract the deduplicated destination VID from GetNeighbors
// Second: Delete previously visited edges and save the result(iter) to the variable `resultVar`
//
// Member:
// `paths_` : a list of hash table
//    KEY in the hash table   : the VID of the visited destination Vertex
//    VALUE in the hash table : the number of steps to visit the KEY (starting vertex is 0)
// since each vertex will only be visited once, if it is a one-way edge expansion, there will be no
// duplicate edges. we only need to focus on the case of two-way expansion
//
// How to delete edges:
//  is judged that a loop is formed, the edge needs to be deleted
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
  using Dst = Value;
  using Paths = std::vector<Row>;
  Status buildRequestDataSet();

  folly::Future<Status> traverse();

  void addStats(RpcResponse& resps, int64_t getNbrTimeInUSec);

  folly::Future<Status> getNeighbors();

  folly::Future<Status> handleResponse(RpcResponse&& resps);

  Status buildInterimPath(GetNeighborsIter* iter);

  Status buildResult();

  bool isFinalStep() const {
    return (range_ == nullptr && currentStep_ == 1) ||
           (range_ != nullptr && (currentStep_ == range_->max() || range_->max() == 0));
  }

  bool zeroStep() const {
    return range_ != nullptr && range_->min() == 0;
  }

  bool hasSameEdge(const Row& prevPath, const Edge& currentEdge);

  void releasePrevPaths(size_t cnt);

  void buildPath(std::unordered_map<Value, std::vector<Row>>& currentPaths,
                 const Value& dst,
                 Row&& path);

  Status handleZeroStep(const std::unordered_map<Value, Paths>& prev,
                        List&& vertices,
                        std::unordered_map<Value, Paths>& zeroSteps,
                        size_t& count);

 private:
  DataSet reqDs_;
  const Traverse* traverse_{nullptr};
  MatchStepRange* range_{nullptr};
  size_t currentStep_{0};
  std::list<std::unordered_map<Value, Paths>> paths_;
  size_t cnt_{0};
};

}  // namespace graph
}  // namespace nebula

#endif  // EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
