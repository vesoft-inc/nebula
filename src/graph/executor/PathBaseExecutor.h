// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_PATHBASE_EXECUTOR_H_
#define GRAPH_EXECUTOR_PATHBASE_EXECUTOR_H_

// #include "graph/planner/plan/Algo.h"
#include "graph/executor/Executor.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using RpcResponse = StorageRpcResponse<GetNeighborsResponse>;
using PropRpcResponse = StorageRpcResponse<nebula::storage::cpp2::GetPropResponse>;
using VertexProp = nebula::storage::cpp2::VertexProp;
using nebula::storage::StorageClient;

namespace nebula {
namespace graph {
class PathBaseExecutor : public Executor {
 public:
  PathBaseExecutor(const std::string& name, const PlanNode* node, QueryContext* qctx)
      : Executor(name, node, qctx) {}

 protected:
  folly::Future<std::vector<Value>> getProps(const std::vector<Value>& vids,
                                             const std::vector<VertexProp>* vertexPropPtr);

  std::vector<Value> handlePropResp(PropRpcResponse&& resps);

  Status handleErrorCode(nebula::cpp2::ErrorCode code, PartitionID partId) const;

  void addGetNeighborStats(RpcResponse& resp, size_t stepNum, int64_t timeInUSec, bool reverse);

  void addGetPropStats(PropRpcResponse& resp, int64_t timeInUSec);

  bool hasSameEdge(const std::vector<Value>& edgeList, const Edge& edge);

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

  struct VertexHash {
    std::size_t operator()(const Value& v) const {
      switch (v.type()) {
        case Value::Type::VERTEX: {
          auto& vid = v.getVertex().vid;
          if (vid.type() == Value::Type::STRING) {
            return std::hash<std::string>()(vid.getStr());
          } else {
            return vid.getInt();
          }
        }
        case Value::Type::STRING: {
          return std::hash<std::string>()(v.getStr());
        }
        case Value::Type::INT: {
          return v.getInt();
        }
        default: {
          return v.hash();
        }
      }
    }
  };

  struct VertexEqual {
    bool operator()(const Value& lhs, const Value& rhs) const {
      if (lhs.type() == rhs.type()) {
        if (lhs.isVertex()) {
          return lhs.getVertex().vid == rhs.getVertex().vid;
        }
        return lhs == rhs;
      }
      if (lhs.type() == Value::Type::VERTEX) {
        return lhs.getVertex().vid == rhs;
      }
      if (rhs.type() == Value::Type::VERTEX) {
        return lhs == rhs.getVertex().vid;
      }
      return lhs == rhs;
    }
  };

 protected:
  folly::SpinLock statsLock_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_PATHBASE_EXECUTOR_H_
