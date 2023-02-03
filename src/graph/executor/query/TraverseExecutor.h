// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
#define EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_

#include <robin_hood.h>

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"

// only used in match scenarios
// invoke the getNeighbors interface, according to the number of times specified by the user,
// and assemble the result into paths
//
//  Path is an array of vertex and edges physically.
//  Its definition is a trail, in which all edges are distinct. It's different to a walk
//  which allows duplicated vertices and edges, and a path where all vertices and edges
//  are distinct.
//
//  Eg a->b->c. path is [Vertex(a), [Edge(a->b), Vertex(b), Edge(b->c), Vertex(c)]]
//  the purpose is to extract the path by pathBuildExpression
// `resDs_` : keep result dataSet
//
// Member:
// `paths_` : hash table array, paths_[i] means that the length that paths in the i-th array
//  element is i
//    KEY in the hash table   : the vid of the destination Vertex
//    VALUE in the hash table : collection of paths that destination vid is `KEY`
//
// Functions:
// `buildRequestDataSet` : constructs the input DataSet for getNeightbors
// `buildInterimPath` : construct collection of paths after expanded and put it into the paths_
// `getNeighbors` : invoke the getNeightbors interface
// `releasePrevPaths` : deleted The path whose length does not meet the user-defined length
// `hasSameEdge` : check if there are duplicate edges in path

namespace nebula {
namespace graph {

using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;

class TraverseExecutor final : public StorageAccessExecutor {
 public:
  TraverseExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("Traverse", node, qctx) {
    traverse_ = asNode<Traverse>(node);
  }

  folly::Future<Status> execute() override;

 private:
  Status buildRequestVids();

  void addStats(RpcResponse& resps, int64_t getNbrTimeInUSec);

  folly::Future<Status> getNeighbors();

  void expand(GetNeighborsIter* iter);

  folly::Future<Status> handleResponse(RpcResponse&& resps);

  folly::Future<Status> buildResult();

  std::vector<Row> buildPath(const Value& initVertex, size_t minStep, size_t maxStep);
  folly::Future<Status> buildPathMultiJobs(size_t minStep, size_t maxStep);
  std::vector<Row> joinPrevPath(const Value& initVertex, const std::vector<Row>& newResult) const;

  bool isFinalStep() const {
    return currentStep_ == range_.max() || range_.max() == 0;
  }

  bool hasSameEdge(const std::vector<Value>& edgeList, const Edge& edge) const;
  bool hasSameEdgeInPath(const Row& lhs, const Row& rhs) const;
  bool hasSameEdgeInSet(const Row& rhs, const std::unordered_set<Value>& uniqueEdge) const;

  std::vector<Row> buildZeroStepPath();

  Expression* selectFilter();

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

 private:
  ObjectPool objPool_;

  std::vector<Value> vids_;
  std::vector<Value> initVertices_;
  DataSet result_;
  // Key : vertex  Value : adjacent edges
  std::unordered_map<Value, std::vector<Value>, VertexHash, VertexEqual> adjList_;
  std::unordered_map<Value, std::vector<Row>, VertexHash, VertexEqual> dst2PathsMap_;
  const Traverse* traverse_{nullptr};
  MatchStepRange range_;
  size_t currentStep_{0};

  size_t expandTime_{0u};
};

}  // namespace graph
}  // namespace nebula

#endif  // EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
