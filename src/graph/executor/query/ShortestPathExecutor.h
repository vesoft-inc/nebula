/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_

#include <unordered_set>
#include <vector>

#include "common/datatypes/DataSet.h"
#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;

class ShortestPathExecutor final : public StorageAccessExecutor {
  enum class VertexType {
    Source = 0,
    Destination,
  };

 public:
  ShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ShortestPath", node, qctx) {
    shortestPathNode_ = asNode<ShortestPath>(node);
  }

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  using Paths = std::vector<Row>;

  Status buildRequestDataSet(std::string inputVar,
                             DataSet& ds,
                             std::unordered_map<Value, Paths>& prev);

  Status buildResult(DataSet result);

  folly::Future<StatusOr<std::vector<DataSet>>> getNeighbors(DataSet& ds, int flag);

  folly::Future<StatusOr<std::vector<DataSet>>> handleResponse(RpcResponse&& resps, int flag);

  void buildPrevPath(std::unordered_map<Value, Paths>& prevPaths, const Value& node, Row&& path);

  void judge();

 private:
  // The result of path;
  size_t cnt_{0};
  std::list<std::unordered_map<Value, Paths>> paths_;

  std::unordered_set<Value> visited_;
  const ShortestPath* shortestPathNode_{nullptr};

  DataSet leftReqDataSet_;
  std::list<std::unordered_map<Value, Paths>> leftPaths_;

  DataSet rightReqDataSet_;

  std::vector<std::unordered_set<Row>> datas_;
  std::vector<std::queue<DataSet>> queues_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
