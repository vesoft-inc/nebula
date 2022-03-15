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
    single_ = shortestPathNode_->single();
    range_ = {shortestPathNode_->stepRange()->min(), shortestPathNode_->stepRange()->max()};
    vids_.reserve(2);
    allPrevPaths_.reserve(2);
    for (int i = 0; i < 2; i++) {
      vids_.emplace_back();
      allPrevPaths_.emplace_back();
    }
  }

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  using PrevNodeEdge = std::vector<Row>;

  Status buildRequestDataSet();

  void setInterimState(int direction);

  void resetPairState();

  Status buildResult();

  folly::Future<Status> getNeighbors();

  folly::Future<Status> handleResponse(RpcResponse&& resps);

  Status judge(GetNeighborsIter* iter);

  void AddPrevPath(std::unordered_map<Value, PrevNodeEdge>& prevPaths,
                   const Value& vid,
                   Row&& prevPath);

  void findPaths(Value nodeVid);

  bool sameEdge(const Value& src, const Edge& edge);

  std::vector<List> getPathsFromMap(Value vid, int direction);

  std::vector<List> filterPathLen(std::vector<List>& paths, int direction);

 private:
  bool single_{true};

  bool break_{false};

  size_t step_{0};
  std::pair<size_t, size_t> range_;
  const ShortestPath* shortestPathNode_{nullptr};

  std::vector<std::pair<Value, Value>> cartesian_;

  DataSet resultDs_;

  // 0: src->dst, 1: dst->src
  int direction_{0};
  // size == 2, left dataset and right dataset;
  std::vector<DataSet> vids_;

  // vid -> Node
  std::unordered_map<Value, Value> vidToVertex_;

  // function: find the path to source/destination
  // {key: prev} key: node, pre: the prev node and prev edge
  std::vector<std::unordered_map<Value, PrevNodeEdge>> allPrevPaths_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
