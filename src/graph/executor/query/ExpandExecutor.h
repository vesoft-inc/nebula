// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_EXPAND_H_
#define GRAPH_EXECUTOR_QUERY_EXPAND_H

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

// Get the dst id of the src id. The dst is is partially deduplicated on the storage side.
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
                             std::unordered_set<Value>& visitedVids);

  folly::Future<Status> expandFromCache();

  void updateDst2VidsMap(std::unordered_map<Value, std::unordered_set<Value>>& dst2VidMap,
                         const Value& src,
                         const Value& dst);

  void sample(const Value& src, const std::unordered_set<Value>& dsts);

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
  // key : edge's dst, value : init vids
  std::unordered_map<Value, std::unordered_set<Value>> preDst2VidsMap_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_ExpandExecutor_H_
