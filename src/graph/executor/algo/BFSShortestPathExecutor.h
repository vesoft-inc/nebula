// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
#include "graph/executor/Executor.h"

// BFSShortestPath has two inputs.  GetNeighbors(From) & GetNeighbors(To)
// There are two Main functions
// First : Get the next vid for GetNeighbors to expand
// Second: Extract edges from GetNeighbors to form path, concatenate the path(From) and the path(To)
//         into a complete path
//
//
// Functions:
// `buildPath`: extract edges from GetNeighbors put it into allLeftEdges or allRightEdges
//   and set the vid that needs to be expanded in the next step
//
// `conjunctPath`: concatenate the path(From) and the path(To) into a complete path
//   allLeftEdges needs to match the previous step of the allRightEdges
//   then current step of the allRightEdges each time
//   Eg. a->b->c->d
//   firstStep:  allLeftEdges [<b, a->b>]  allRightEdges [<c, d<-c>],   can't find common vid
//   secondStep: allLeftEdges [<b, a->b>, <c, b->c>] allRightEdges [<b, c<-b>, <c, d<-c>]
//   we should use allLeftEdges(secondStep) to match allRightEdges(firstStep) first
//   if find common vid, no need to match allRightEdges(secondStep)
//
// Member:
// `allLeftEdges_` : is a array, each element in the array is a hashTable
//  hash table
//    KEY   : the VID of the vertex
//    VALUE : edges (the destination is KEY)
//
// `allRightEdges_` : same as allLeftEdges_
//
// `leftVisitedVids_` : keep already visited vid to avoid repeated visits (left)
// `rightVisitedVids_` : keep already visited vid to avoid repeated visits (right)
// `leftVidVar_` : getNeighbors(left)'s inputVar
// `rightVidVar_` : getNeighbors(right)'s inputVar
namespace nebula {
namespace graph {
class BFSShortestPathExecutor final : public Executor {
 public:
  BFSShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("BFSShortestPath", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  void buildPath(Iterator* iter, bool reverse);

  std::vector<Row> conjunctPath();

  std::unordered_multimap<Value, Path> createPath(std::vector<Value> meetVids,
                                                  bool reverse,
                                                  bool oddStep);

 private:
  size_t step_{1};
  size_t steps_{1};
  std::string leftVidVar_;
  std::string rightVidVar_;
  std::unordered_set<Value> leftVisitedVids_;
  std::unordered_set<Value> rightVisitedVids_;
  std::vector<std::unordered_multimap<Value, Edge>> allLeftEdges_;
  std::vector<std::unordered_multimap<Value, Edge>> allRightEdges_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
