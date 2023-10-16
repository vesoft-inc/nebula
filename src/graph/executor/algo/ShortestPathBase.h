// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_ALGO_SHORTESTPATHBASE_H_
#define GRAPH_EXECUTOR_ALGO_SHORTESTPATHBASE_H_

#include <robin_hood.h>

#include "graph/executor/Executor.h"
#include "graph/planner/plan/Algo.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using RpcResponse = StorageRpcResponse<GetNeighborsResponse>;
using PropRpcResponse = StorageRpcResponse<nebula::storage::cpp2::GetPropResponse>;

namespace nebula {
namespace graph {

class ShortestPathBase {
 public:
  using HashSet = robin_hood::unordered_flat_set<Value, std::hash<Value>>;
  ShortestPathBase(const ShortestPath* node,
                   QueryContext* qctx,
                   std::unordered_map<std::string, std::string>* stats)
      : pathNode_(node), qctx_(qctx), stats_(stats) {
    singleShortest_ = node->singleShortest();
    maxStep_ = node->stepRange().max();
  }

  virtual ~ShortestPathBase() {}

  virtual folly::Future<Status> execute(const HashSet& startVids,
                                        const HashSet& endVids,
                                        DataSet* result) = 0;

  using DstVid = Value;
  // start vid of the path
  using StartVid = Value;
  // end vid of the path
  using EndVid = Value;
  // save the starting vertex and the corresponding edge. eg [vertex(a), edge(a->b)]
  using CustomStep = Row;

 protected:
  folly::Future<std::vector<Value>> getMeetVidsProps(const std::vector<Value>& meetVids);

  std::vector<Value> handlePropResp(PropRpcResponse&& resps);

  Status handleErrorCode(nebula::cpp2::ErrorCode code, PartitionID partId) const;

  void addStats(RpcResponse& resp, size_t stepNum, int64_t timeInUSec, bool reverse) const;

  void addStats(PropRpcResponse& resp, int64_t timeInUSec) const;

  bool hasSameEdge(const std::vector<Value>& values);

  folly::Executor* runner() const;

  template <typename Resp>
  StatusOr<Result::State> handleCompleteness(const storage::StorageRpcResponse<Resp>& rpcResp,
                                             bool isPartialSuccessAccepted) const {
    auto completeness = rpcResp.completeness();
    if (completeness != 100) {
      const auto& failedCodes = rpcResp.failedParts();
      for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
        LOG(ERROR) << " failed, error " << apache::thrift::util::enumNameSafe(it->second)
                   << ", part " << it->first;
      }
      // cannot execute at all, or partial success is not accepted
      if (completeness == 0 || !isPartialSuccessAccepted) {
        if (failedCodes.size() > 0) {
          return handleErrorCode(failedCodes.begin()->second, failedCodes.begin()->first);
        }
        return Status::Error("Request to storage failed, without failedCodes.");
      }
      // partial success is accepted
      qctx_->setPartialSuccess();
      return Result::State::kPartialSuccess;
    }
    return Result::State::kSuccess;
  }

 protected:
  const ShortestPath* pathNode_{nullptr};
  QueryContext* qctx_{nullptr};
  std::unordered_map<std::string, std::string>* stats_{nullptr};
  size_t maxStep_;
  bool singleShortest_{true};
  folly::SpinLock statsLock_;

  std::vector<DataSet> resultDs_;
  std::vector<HashSet> leftVids_;
  std::vector<HashSet> rightVids_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_ShortestPathBase_H_
