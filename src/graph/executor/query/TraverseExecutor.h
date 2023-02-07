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
//  Path is an array of vertex and edges physically.
//  Its definition is a trail, in which all edges are distinct. It's different to a walk
//  which allows duplicated vertices and edges, and a path where all vertices and edges
//  are distinct.
//
//  Eg a->b->c. path is [Vertex(a), [Edge(a->b), Vertex(b), Edge(b->c), Vertex(c)]]
//  the purpose is to extract the path by pathBuildExpression
// `resDs_` : keep result dataSet
//
// Member:
// `paths_` : hash table array, paths_[i] means that the length that paths in the i-th array
//  element is i
//    KEY in the hash table   : the vid of the destination Vertex
//    VALUE in the hash table : collection of paths that destination vid is `KEY`
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

  template <typename T = Value>
  using VertexMap = std::unordered_map<Value, std::vector<T>, VertexHash, VertexEqual>;

 private:
  Status buildRequestVids();

  void addStats(RpcResponse& resps, int64_t getNbrTimeInUSec);

  folly::Future<Status> getNeighbors();

  size_t numRowsOfRpcResp(const RpcResponse& resps) const;

  void expand(GetNeighborsIter* iter);
  void buildAdjList(DataSet& dataset,
                    std::vector<Value>& initVertices,
                    VidHashSet& vids,
                    VertexMap<Value>& adjList) const;
  folly::Future<Status> expandOneStep(RpcResponse&& resps);
  folly::Future<Status> asyncExpandOneStep(RpcResponse&& resps);
  folly::Future<Status> handleResponse(RpcResponse&& resps);

  folly::Future<Status> buildResult();

  std::vector<Row> buildPath(const Value& initVertex, size_t minStep, size_t maxStep);
  folly::Future<Status> buildPathMultiJobs(size_t minStep, size_t maxStep);
  std::vector<Row> joinPrevPath(const Value& initVertex, const std::vector<Row>& newResult) const;

  bool isFinalStep() const {
    return currentStep_ == range_.max() || range_.max() == 0;
  }

  bool hasSameEdge(const std::vector<Value>& edgeList, const Edge& edge) const;
  bool hasSameEdgeInPath(const Row& lhs, const Row& rhs) const;
  bool hasSameEdgeInSet(const Row& rhs, const std::unordered_set<Value>& uniqueEdge) const;

  std::vector<Row> buildZeroStepPath();

  Expression* selectFilter();

 private:
  ObjectPool objPool_;

  VidHashSet vids_;
  std::vector<Value> initVertices_;
  DataSet result_;
  // Key : vertex  Value : adjacent edges
  VertexMap<Value> adjList_;
  VertexMap<Row> dst2PathsMap_;
  const Traverse* traverse_{nullptr};
  MatchStepRange range_;
  size_t currentStep_{0};
};

}  // namespace graph
}  // namespace nebula

#endif  // EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
