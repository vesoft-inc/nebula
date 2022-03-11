// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_

#include "graph/executor/Executor.h"

// FindPath has two inputs.  GetNeighbors(From) & GetNeighbors(To)
// There are two Main functions
// First : Get the next vid for GetNeighbors to expand
// Second: Delete previously visited edges and save the result(iter) to the variable `resultVar`
//
// Member:
// `preLeftPaths_` : is hash table
//    KEY   : the VID of the visited destination Vertex
//    VALUE : the number of steps to visit the KEY (starting vertex is 0)
// since each vertex will only be visited once, if it is a one-way edge expansion, there will be no
// duplicate edges. we only need to focus on the case of two-way expansion
//
// How to delete edges:
//  determine whether a loop is formed by the number of steps. If the destination vid has been
//  visited, and the number of steps of the destination vid differs by 2 from the current steps, it
//  is judged that a loop is formed, the edge needs to be deleted
//
// For example: Topology is below
// a->c, a->b, b->a, b->c
// statement: get subgraph from 'a' both edge yield vertices as nodes, edges as relationships
// first steps :  a->b, a->c, a<-b, all edges need to save
// second steps:  b->a, b<-a, b->c, c<-a
// since it is a two-way expansion, the negative edge has already been visited,
// so b<-a & c<-a are deleted
// b->a : the number of steps of the destination vid `a` is 0, and the current steps is 2. it can be
// determined that a loop is formed, so this edge also needs to be deleted.
// b->c : determined by the number of steps that no loop is formed, so keep it
namespace nebula {
namespace graph {
class FindPathExecutor final : public Executor {
 public:
  FindPathExecutor(const PlanNode* node, QueryContext* qctx) : Executor("FindPath", node, qctx) {}

  folly::Future<Status> execute() override;

  // key : dst, value: <path>
  using Interims = std::unordered_map<Value, std::vector<Path>>;

 private:
  void init();
  bool conjunctPath(Interims& leftPaths, Interims& rightPaths, DataSet& ds);
  void setNextStepVid(Interims& paths, const std::string& var);
  void buildPath(Iterator* iter, Interims& currentPath, bool reverse);

 private:
  bool shortest_{false};
  bool noLoop_{false};
  // current step
  size_t step_{1};
  std::string terminationVar_;
  std::unordered_multimap<Value, std::pair<Value, bool>> terminationMap_;

  Interims preLeftPaths_;
  Interims preRightPaths_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_
