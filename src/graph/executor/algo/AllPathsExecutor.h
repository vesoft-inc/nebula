// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_ALLPATHSEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_ALLPATHSEXECUTOR_H_

#include "graph/executor/PathBaseExecutor.h"

namespace nebula {
namespace graph {
class AllPaths;
class AllPathsExecutor final : public PathBaseExecutor {
 public:
  AllPathsExecutor(const PlanNode* node, QueryContext* qctx)
      : PathBaseExecutor("AllPaths", node, qctx) {}

  folly::Future<Status> execute() override;

  enum class Direction : uint8_t {
    kLeft,
    kRight,
    kBoth,
  };

 private:
  void init();

  Direction direction();

  folly::Future<Status> doAllPaths();

  folly::Future<Status> getNeighbors(bool reverse);

  void expandFromLeft(GetNeighborsIter* iter);

  void expandFromRight(GetNeighborsIter* iter);

  std::vector<Row> doBuildPath(const Value& vid);

  void buildPath();

  folly::Future<Status> buildResult();

 private:
  const AllPaths* pathNode_{nullptr};
  bool withProp_{false};
  bool noLoop_{false};
  size_t limit_{0};
  size_t steps_{0};

  size_t leftSteps_{0};
  size_t rightSteps_{0};

  std::vector<Value> leftVids_;
  std::vector<Value> rightVids_;
  std::unordered_set<Value> leftInitVids_;
  std::unordered_set<Value, VertexHash, VertexEqual> rightInitVids_;

  std::unordered_map<Value, std::vector<Value>, VertexHash, VertexEqual> leftAdjList_;
  std::unordered_map<Value, std::vector<Value>, VertexHash, VertexEqual> rightAdjList_;

  DataSet result_;
  std::vector<Value> emptyPropVids_;
};
}  // namespace graph
}  // namespace nebula
#endif
