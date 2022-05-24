// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_QUERY_SHORTESTPATHBASE_H_
#define GRAPH_EXECUTOR_QUERY_SHORTESTPATHBASE_H_

#include "graph/planner/plan/Algo.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using RpcResponse = StorageRpcResponse<GetNeighborsResponse>;
using PropRpcResponse = StorageRpcResponse<nebula::storage::cpp2::GetPropResponse>;
namespace nebula {
namespace graph {
class ShortestPathBase {
 public:
  ShortestPathBase(const ShortestPath* node,
                   QueryContext* qctx,
                   std::unordered_map<std::string, std::string>* stats)
      : pathNode_(node), qctx_(qctx), stats_(stats) {
    singleShortest_ = node->singleShortest();
    maxStep_ = node->stepRange()->max();
  }

  virtual ~ShortestPathBase() {}

  virtual folly::Future<Status> execute(const std::unordered_set<Value>& startVids,
                                        const std::unordered_set<Value>& endVids,
                                        DataSet* result) = 0;

  using DstVid = Value;
  // start vid of the path
  using StartVid = Value;
  // end vid of the path
  using EndVid = Value;
  // save the starting vertex and the corresponding edge. eg [vertex(a), edge(a->b)]
  using CustomStep = Row;

 protected:
  Status handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices);

  folly::Future<Status> getMeetVidsProps(const std::vector<Value>& meetVids,
                                         std::vector<Value>& meetVertices);

  Status handleErrorCode(nebula::cpp2::ErrorCode code, PartitionID partId) const;

  template <typename RESP>
  void addStats(RESP& resp, std::unordered_map<std::string, std::string>* stats) const {
    auto& hostLatency = resp.hostLatency();
    for (size_t i = 0; i < hostLatency.size(); ++i) {
      auto& info = hostLatency[i];
      stats->emplace(folly::sformat("{} exec/total", std::get<0>(info).toString()),
                     folly::sformat("{}(us)/{}(us)", std::get<1>(info), std::get<2>(info)));
      auto detail = getStorageDetail(resp.responses()[i].result_ref()->latency_detail_us_ref());
      if (!detail.empty()) {
        stats->emplace("storage_detail", detail);
      }
    }
  }

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

  std::string getStorageDetail(
      apache::thrift::optional_field_ref<const std::map<std::string, int32_t>&> ref) const;

 protected:
  const ShortestPath* pathNode_{nullptr};
  QueryContext* qctx_{nullptr};
  std::unordered_map<std::string, std::string>* stats_{nullptr};
  size_t maxStep_;
  bool singleShortest_{true};

  std::vector<DataSet> resultDs_;
  std::vector<DataSet> leftVids_;
  std::vector<DataSet> rightVids_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_ShortestPathBase_H_
