// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_ALLPATHSEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_ALLPATHSEXECUTOR_H_

#include "graph/executor/Executor.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using RpcResponse = StorageRpcResponse<GetNeighborsResponse>;
using PropRpcResponse = StorageRpcResponse<nebula::storage::cpp2::GetPropResponse>;
using nebula::storage::StorageClient;

namespace nebula {
namespace graph {
class AllPaths;
class AllPathsExecutor final : public Executor {
 public:
  AllPathsExecutor(const PlanNode* node, QueryContext* qctx) : Executor("AllPaths", node, qctx) {}

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

  bool hasSameEdge(const std::vector<Value>& edgeList, const Edge& edge);

  std::vector<Value> handlePropResp(PropRpcResponse&& resps);

  folly::Future<std::vector<Value>> getProps(const std::vector<Value>& vids);

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

 private:
  const AllPaths* pathNode_{nullptr};
  bool withProp_{false};
  bool noLoop_{false};
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
  // folly::SpinLock statsLock_;
};
}  // namespace graph
}  // namespace nebula
#endif
