// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
#define EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"
// only used in match scenarios
// invoke the getNeighbors interface, according to the number of times specified by the user,
// and assemble the result into paths
//
//  The definition of path is : array of vertex and edges
//  Eg a->b->c. path is [Vertex(a), [Edge(a->b), Vertex(b), Edge(b->c), Vertex(c)]]
//  the purpose is to extract the path by pathBuildExpression
// `resDs_` : keep result dataSet
//
// Member:
// `paths_` : hash table array, paths_[i] means that the length that paths in the i-th array
//  element is i
//    KEY in the hash table   : the vid of the destination Vertex
//    VALUE in the hash table : collection of paths that destionation vid is `KEY`
//
// Functions:
// `buildRequestDataSet` : constructs the input DataSet for getNeightbors
// `buildInterimPath` : construct collection of paths after expanded and put it into the paths_
// `getNeighbors` : invoke the getNeightbors interface
// `releasePrevPaths` : deleted The path whose length does not meet the user-defined length
// `hasSameEdge` : check if there are duplicate edges in path
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
    return node_->asNode<Traverse>()->zeroStep();
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
