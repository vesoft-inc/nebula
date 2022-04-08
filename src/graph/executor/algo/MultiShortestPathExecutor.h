// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_MULTISHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_MULTISHORTESTPATHEXECUTOR_H_

#include "graph/executor/Executor.h"
// MultiShortestPath has two inputs.  GetNeighbors(From) & GetNeighbors(To)
// There are two Main functions
// First : Get the next vid for GetNeighbors to expand
// Second: Extract edges from GetNeighbors to form path, concatenate the path(From) and the path(To)
//         into a complete path
//
// Since FromVid & ToVid are expanded at the same time
// the paths(From) need to be spliced ​​with the path(To) of the previous step,
// and then spliced ​​with the current path(To)
//
// Functions:
// `init`: initialize preRightPaths_ & terminationMap_
//
// `buildPath`: extract edges from GetNeighbors to form path (use previous paths)
//
// `conjunctPath`: concatenate the path(From) and the path(To) into a complete path
//   leftPaths needs to match the previous step of the rightPaths
//   then current step of the rightPaths each time
//   Eg. a->b->c->d
//   firstStep:  leftPath [<b, a->b>]  rightPath [<c, d<-c>],   can't find common vid
//   secondStep: leftPath [<c, a->b->c>] rightPath [<b, <d<-c<-b>]
//   we should use leftPath(secondStep) to match rightPath(firstStep) first
//   if find common vid, no need to match rightPath(secondStep)
//
// `setNextStepVid`: set the vid that needs to be expanded in the next step

// Member:
// `preLeftPaths_` : is hash table (only keep the previous step)
//    KEY   : the VID of the vertex
//    VALUE : all paths(the destination is KEY)
//
// `preRightPaths_` : same as preLeftPaths_
// `terminationMap_` is hash table, cartesian product of From & To vid, In shortest path scenarios,
//  when a pair of paths is found, the pair of data is deleted, and when it is empty, the expansion
//  is terminated
// `terminationVar_`: when terminationMap_ is empty, then all paths are found, set it to true and
//  the loop will terminate
// `leftPaths_` : same as preLeftPaths_(only keep the current step)
// `rightPaths_` : same as preRightPaths_(only keep the current step)
// `currentDs_`: keep the paths matched in current step
namespace nebula {
namespace graph {
class MultiShortestPath;
class MultiShortestPathExecutor final : public Executor {
 public:
  MultiShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("MultiShortestPath", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  // k: dst, v: paths to dst
  using Interims = std::unordered_map<Value, std::vector<Path>>;

  void init();
  Status buildPath(bool reverse);
  folly::Future<bool> conjunctPath(bool oddStep);
  DataSet doConjunct(Interims::iterator startIter, Interims::iterator endIter, bool oddStep);
  void setNextStepVid(const Interims& paths, const string& var);

 private:
  const MultiShortestPath* pathNode_{nullptr};
  size_t step_{1};
  std::string terminationVar_;
  std::unordered_multimap<Value, std::pair<Value, bool>> terminationMap_;
  Interims leftPaths_;
  Interims preLeftPaths_;
  Interims preRightPaths_;
  Interims rightPaths_;
  DataSet currentDs_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_MULTISHORTESTPATHEXECUTOR_H_
