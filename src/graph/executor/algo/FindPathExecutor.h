// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_

#include "graph/executor/Executor.h"

// FindPath has two inputs.  GetNeighbors(From) & GetNeighbors(To)
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
//
// `setNextStepVid`: set the vid that needs to be expanded in the next step

// Member:
// `preLeftPaths_` : is hash table (only keep the previous step)
//    KEY   : the VID of the vertex
//    VALUE : all paths(the destination is KEY)
//
// `preRightPaths_` : same as preLeftPaths_
//
// `terminationMap_` is hash table, cartesian product of From & To vid, In shortest path scenarios,
//  when a pair of paths is found, the pair of data is deleted, and when it is empty, the expansion
//  is terminated
//
// `terminationVar_`: when terminationMap_ is empty, then all paths are found, set it to true and
//  the loop will terminate
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
