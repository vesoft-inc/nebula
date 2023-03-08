// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_EXPAND_ALL_H_
#define GRAPH_EXECUTOR_QUERY_EXPAND_ALL_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

// The go statement is divided into two operators, expand operator and expandAll operator.
// expandAll is responsible for expansion and take attributes that user need.

// The semantics of GO N STEPS FROM 'xxx' OVER edge WHERE condition YIELD yyy is
// First : expand n-1 steps from 'xxx' to get the result set vids(deduplication)
// Second: expand the last step from vids. get the attributes required by the users during expansion
//         then execute filter condition. finally return the results to the next operator

// The semantics of GO M TO N STEPS is
// GO M STEPS
// UNION ALL
// GO M+1 STEPS
// ...
// UNION ALL
// GO N STEPS

// therefore. each step in expandAll operator. we need adds the result to the global dataset result_
// and returns the result_ after the expansion is completed.

// if need to join with the previous statement, we need save the mapping relationship
// between the init vid and the destination vid during the expansion
// finally add a column (column name `expand_vid`, store the init vids) to the result_
// for join previous statement

// if expression contains $$.tag.propName、 $$
// we need add a column(colume name `_expandall_dst`, store the destination vid)
// for join getVertices's dataset

// adjList is an adjacency list structure
// which saves the vids and the attributes of edge and src vertex that user need when expasion
// when expanding, if the vid has already been visited, do not need to go through RPC
// just get the result directly through adjList_

// when minSteps == maxSteps, only need to expand one step
// then filter and limit information can be pushed down to storage

namespace nebula {
namespace graph {
class ExpandAllExecutor final : public StorageAccessExecutor {
 public:
  ExpandAllExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ExpandAllExecutor", node, qctx) {
    expand_ = asNode<ExpandAll>(node);
  }

  Status buildRequestVids();

  folly::Future<Status> execute() override;

  folly::Future<Status> getNeighbors();

  void getNeighborsFromCache(std::unordered_map<Value, std::unordered_set<Value>>& dst2VidsMap,
                             std::unordered_set<Value>& visitedVids,
                             std::vector<int64_t>& samples);

  folly::Future<Status> expandFromCache();

  void updateDst2VidsMap(std::unordered_map<Value, std::unordered_set<Value>>& dst2VidMap,
                         const Value& src,
                         const Value& dst);

  void buildResult(const List& vList, const List& eList);

  void buildResult(const std::unordered_set<Value>& vids, const List& vList, const List& eList);

  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
  folly::Future<Status> handleResponse(RpcResponse&& resps);

  void resetNextStepVids(std::unordered_set<Value>& visitedVids);

 private:
  const ExpandAll* expand_;
  bool joinInput_{false};
  size_t currentStep_{0};
  size_t maxSteps_{0};
  DataSet result_;
  YieldColumns* edgeColumns_{nullptr};
  YieldColumns* vertexColumns_{nullptr};

  bool sample_{false};
  int64_t curLimit_{0};
  int64_t curMaxLimit_{std::numeric_limits<int64_t>::max()};
  std::vector<int64_t> stepLimits_;

  std::unordered_set<Value> nextStepVids_;
  std::unordered_set<Value> preVisitedVids_;
  std::unordered_map<Value, std::vector<List>> adjList_;
  // keep the mapping relationship between the init vid and the destination vid
  // during the expansion.  KEY : edge's dst, VALUE : init vids
  // then we can know which init vids can reach the current destination point
  std::unordered_map<Value, std::unordered_set<Value>> preDst2VidsMap_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_ExpandAllExecutor_H_
