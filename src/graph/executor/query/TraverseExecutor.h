// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
#define EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_

#include <robin_hood.h>

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
using Dst = Value;
using Paths = std::vector<Row>;
using HashSet = robin_hood::unordered_flat_set<Value, std::hash<Value>>;
using HashMap = robin_hood::unordered_flat_map<Dst, Paths, std::hash<Value>>;

struct JobResult {
  // Newly traversed paths size
  size_t pathCnt{0};
  // Request vids for next traverse
  HashSet vids;
  // Newly traversed paths
  HashMap newPaths;
};

class TraverseExecutor final : public StorageAccessExecutor {
 public:
  TraverseExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("Traverse", node, qctx) {
    traverse_ = asNode<Traverse>(node);
  }

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  Status buildRequestVids();

  folly::Future<Status> traverse();

  void addStats(RpcResponse& resps, int64_t getNbrTimeInUSec);

  folly::Future<Status> getNeighbors();

  folly::Future<Status> handleResponse(RpcResponse&& resps);

  Status buildInterimPath(GetNeighborsIter* iter);

  folly::Future<Status> buildInterimPathMultiJobs(std::unique_ptr<GetNeighborsIter> iter);

  StatusOr<JobResult> handleJob(size_t begin, size_t end, Iterator* iter, const HashMap& prev);

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

  void buildPath(HashMap& currentPaths, const Value& dst, Row&& path);

  Status handleZeroStep(const HashMap& prev, List&& vertices, HashMap& zeroSteps, size_t& count);

  Expression* selectFilter();

 private:
  ObjectPool objPool_;
  HashSet vids_;
  const Traverse* traverse_{nullptr};
  MatchStepRange* range_{nullptr};
  size_t currentStep_{0};
  std::list<HashMap> paths_;
  size_t totalPathCnt_{0};
};

}  // namespace graph
}  // namespace nebula

#endif  // EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
