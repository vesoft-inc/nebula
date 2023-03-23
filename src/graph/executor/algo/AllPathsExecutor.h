// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_ALLPATHSEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_ALLPATHSEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"

// Using the two-way BFS algorithm, a heuristic algorithm is used in the expansion process
// when the number of vid to be expanded on the left and right
// exceeds the threshold(FLAGS_path_threshold_size)

// if size(leftVids) / size(rightVids) >= FLAGS_path_threshold_ratio(default 2)
//    expandFromRight
// else if size(rightVids) / size(leftVids) >= FLAGS_path_threshold_ratio(default 2)
//    expandFromLeft
// else
//    expandFromLeft
//    expandFromRight
// this way can avoid uneven calculation distribution due to data skew
// finally the path is constructed using an asynchronous process in the adjacency list

// adjList is an adjacency list structure
// which saves the vids and all adjacent edges that expand one step
// when expanding, if the vid has already been visited, do not visit again
// leftAdjList_ save result of forward expansion
// rightAdjList_ save result of backward expansion

namespace nebula {
namespace graph {
class AllPaths;
class AllPathsExecutor final : public StorageAccessExecutor {
 public:
  AllPathsExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("AllPaths", node, qctx) {}

  folly::Future<Status> execute() override;

  enum class Direction : uint8_t {
    kLeft,
    kRight,
    kBoth,
  };

  template <typename T = Value>
  using VertexMap = std::unordered_map<Value, std::vector<T>, VertexHash, VertexEqual>;

 private:
  void buildRequestVids(bool reverse);

  Direction direction();

  folly::Future<Status> doAllPaths();

  folly::Future<Status> getNeighbors(bool reverse);

  void expandFromLeft(GetNeighborsIter* iter);

  void expandFromRight(GetNeighborsIter* iter);

  folly::Future<std::vector<Row>> doBuildPath(
      size_t step,
      size_t start,
      size_t end,
      std::shared_ptr<std::vector<std::vector<Value>>> edgeLists);

  folly::Future<Status> getPathProps();

  folly::Future<Status> buildPathMultiJobs();

  folly::Future<Status> buildResult();

  bool hasSameVertices(const std::vector<Value>& edgeList, const Edge& edge);

 private:
  const AllPaths* pathNode_{nullptr};
  bool withProp_{false};
  bool noLoop_{false};
  size_t limit_{std::numeric_limits<size_t>::max()};
  std::atomic<size_t> cnt_{0};
  size_t maxStep_{0};

  size_t leftSteps_{0};
  size_t rightSteps_{0};

  std::vector<Value> leftNextStepVids_;
  std::vector<Value> rightNextStepVids_;
  VidHashSet leftInitVids_;
  VidHashSet rightInitVids_;

  VertexMap<Value> leftAdjList_;
  VertexMap<Value> rightAdjList_;

  DataSet result_;
  std::vector<Value> emptyPropVids_;
};
}  // namespace graph
}  // namespace nebula
#endif
