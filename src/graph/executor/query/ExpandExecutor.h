// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_EXPAND_H_
#define GRAPH_EXECUTOR_QUERY_EXPAND_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

// The go statement is divided into two operators, expand operator and expandAll operator.
// expand is responsible for expansion and does not take attributes.

// if no need join, invoke the getDstBySrc interface to output only one column,
// which is the set of destination vids(deduplication) after maxSteps expansion

// if need to join with the previous statement, invoke the getNeighbors interface,
// and we need save the mapping relationship between the init vid and the destination vid
// during the expansion. finally output two columns, the first column is the init vid
// the second column is the destination vid after maxSteps expansion from this vid

// If maxSteps == 0, no expansion, and output after checking the type of vids

// adjList is an adjacency list structure
// which saves the vids and all destination vids that expand one step
// when expanding, if the vid has already been visited, do not need to go through RPC
// just get the result directly through adjList_

namespace nebula {
namespace graph {
class ExpandExecutor final : public StorageAccessExecutor {
 public:
  ExpandExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ExpandExecutor", node, qctx) {
    expand_ = asNode<Expand>(node);
  }

  Status buildRequestVids();

  folly::Future<Status> execute() override;

  folly::Future<Status> getNeighbors();

  folly::Future<Status> GetDstBySrc();

  void getNeighborsFromCache(std::unordered_map<Value, std::unordered_set<Value>>& dst2VidMap,
                             std::unordered_set<Value>& visitedVids,
                             std::vector<int64_t>& samples);

  folly::Future<Status> expandFromCache();

  void updateDst2VidsMap(std::unordered_map<Value, std::unordered_set<Value>>& dst2VidMap,
                         const Value& src,
                         const Value& dst);

  folly::Future<Status> buildResult();

  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
  folly::Future<Status> handleResponse(RpcResponse&& resps);

 private:
  const Expand* expand_;
  size_t currentStep_{0};
  size_t maxSteps_{0};

  bool sample_{false};
  int64_t curLimit_{0};
  int64_t curMaxLimit_{std::numeric_limits<int64_t>::max()};
  std::vector<int64_t> limits_;

  std::unordered_set<Value> nextStepVids_;
  std::unordered_set<Value> preVisitedVids_;
  std::unordered_map<Value, std::unordered_set<Value>> adjDsts_;

  // keep the mapping relationship between the init vid and the destination vid
  // during the expansion.  KEY : edge's dst, VALUE : init vids
  // then we can know which init vids can reach the current destination point
  std::unordered_map<Value, std::unordered_set<Value>> preDst2VidsMap_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_ExpandExecutor_H_
