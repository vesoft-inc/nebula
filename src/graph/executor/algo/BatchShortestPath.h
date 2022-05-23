// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_ALGO_BATCHSHORTESTPATH_H_
#define GRAPH_EXECUTOR_ALGO_BATCHSHORTESTPATH_H_

#include "graph/executor/algo/ShortestPathBase.h"
#include "graph/planner/plan/Algo.h"

// ShortestPath receive result from BiCaresianProduct(Construct by two match node variables)
// The logic is as follows
// Traverse the data in the CartesianProduct, and call the shortestPath function for each line
// each call to the shortestPath function uses a thread to execute. then write result(matched path)
// to the index corresponding to the resultDs_ array. finally collect the data in the resultDs_
// array in the collect stage and return
//
// Functions:
// `buildRequestDataSet`: initialize the member variable and set the starting vid for getNeightbor
//
// `getNeightbor`: invoke the getNeightbor interface and save the result
//
// `shortestPath`: calculate the shortest path between two vids
//
// `conjunctPath`: concatenate the path(From) and the path(To) into a complete path
//   allLeftSteps needs to match the previous step of the allRightSteps
//   then current step of the allRightEdges each time
//   Eg. a->b->c->d
//   firstStep:  allLeftEdges [<b, a->b>]  allRightEdges [<c, d<-c>],   can't find common vid
//   secondStep: allLeftEdges [<b, a->b>, <c, b->c>] allRightEdges [<b, c<-b>, <c, d<-c>]
//   we should use allLeftEdges(secondStep) to match allRightEdges(firstStep) first
//   if find common vid, no need to match allRightEdges(secondStep)
//
// `createLeftPath` : construct a path using backtracking from the allLeftPaths_[i] array
//
// `createRightPath`: same as createLeftPath
//
// `getMeetVidsProps`: get the properties of the meetVids by calling getProps interface
// when the path's steps is even
//
// Member:
// `resultDs_[i]`: keep the paths matched
// `leftVids_[i]` : keep vids to be expanded by GetNeightbor (left)
// `rightVids_[i]` : keep vids to be expanded by GetNeightbor (right)
// `leftVisitedVids_[i]` : keep already visited vid to avoid repeated visits (left)
// `rightVisitedVids_[i]` : keep already visited vid to avoid repeated visits (right)
//
// `allLeftPaths_` : is a array.
// `allLeftPaths_[i]` : is a array, each element in the array is a hashTable
//  hash table
//    KEY   : the VID of the vertex(dst)
//    VALUE : is Row[vertex(src), edge].
//
// `allRightPaths_` : same as allLeftPaths_
namespace nebula {
namespace graph {
class BatchShortestPath final : public ShortestPathBase {
 public:
  BatchShortestPath(const ShortestPath* node,
                    QueryContext* qctx,
                    std::unordered_map<std::string, std::string>* stats)
      : ShortestPathBase(node, qctx, stats) {}

  folly::Future<Status> execute(const std::unordered_set<Value>& startVids,
                                const std::unordered_set<Value>& endVids,
                                DataSet* result) override;

  using VidMap = std::unordered_map<DstVid, std::unordered_set<StartVid>>;

  using HalfPath =
      std::unordered_map<DstVid, std::unordered_map<StartVid, std::vector<CustomStep>>>;

 private:
  size_t splitTask(const std::unordered_set<Value>& startVids,
                   const std::unordered_set<Value>& endVids);

  size_t init(const std::unordered_set<Value>& startVids, const std::unordered_set<Value>& endVids);

  folly::Future<Status> getNeighbors(size_t rowNum, bool reverse);

  folly::Future<Status> shortestPath(size_t rowNum, size_t stepNum);

  folly::Future<Status> handleResponse(size_t rowNum, size_t stepNum);


  Status buildPath(size_t rowNum, RpcResponse&& resp, bool reverse);

  Status doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  bool conjunctPath(size_t rowNum, size_t stepNum);

  bool buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids);

  bool buildOddPath(size_t rowNum, const std::vector<Value>& meetVids);

  bool checkMeetVid(const Value& meetVid, size_t rowNum);

  bool updateTermination(size_t rowNum);

 private:
  std::vector<VidMap> allLeftVidMap_;
  std::vector<VidMap> allRightVidMap_;
  std::vector<std::vector<StartVid>> batchStartVids_;
  std::vector<std::vector<EndVid>> batchEndVids_;
  std::vector<std::unordered_multimap<Value, std::pair<Value, bool>>> terminationMaps_;
  std::vector<HalfPath> allLeftPaths_;
  std::vector<HalfPath> allRightPaths_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ALGO_BATCHSHORTESTPATH_H_
