/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_

#include "common/datatypes/Value.h"
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
    dss_.reserve(2);
    for (int i = 0; i < 2; i++) {
      dss_.emplace_back();
      visiteds_.emplace_back();
    }
  }

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  using Paths = std::vector<Row>;

  Status buildRequestDataSet();

  Status buildResult(DataSet result);

  folly::Future<Status> getNeighbors();

  folly::Future<Status> handleResponse(RpcResponse&& resps);

  Status judge(GetNeighborsIter* iter);

  void AddPrevPath(std::unordered_map<Value, Paths>& prevPaths, const Value& vid, Row&& prevPath);

  DataSet findPaths(Value nodeVid);

 private:
  int step_{0};
  // 0: src->dst, 1: dst->src
  int direction_{0};

  // The dataset param of getNeighbors()
  DataSet reqDs_;

  const ShortestPath* shortestPathNode_{nullptr};

  // size == 2, left dataset and right dataset;
  std::vector<DataSet> dss_;

  std::vector<std::unordered_set<Value>> visiteds_;

  // vid -> Node
  std::unordered_map<Value, Value> vidToVertex_;

  // function: find the path to source/destination
  // {key: path} key: node, path: the prev node of path to src/dst node
  std::unordered_map<Value, Paths> leftPaths_;
  std::unordered_map<Value, Paths> rightPaths_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
