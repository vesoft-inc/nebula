// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
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
// `createLeftPath` : construct a path using backtracking from the allLeftSteps_[i] array
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
// `allLeftSteps_` : is a array.
// `allLeftSteps_[i]` : is a array, each element in the array is a hashTable
//  hash table
//    KEY   : the VID of the vertex(dst)
//    VALUE : is Row[vertex(src), edge].
//
// `allRightSteps_` : same as allLeftEdges_
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using RpcResponse = StorageRpcResponse<GetNeighborsResponse>;
using PropRpcResponse = StorageRpcResponse<nebula::storage::cpp2::GetPropResponse>;
namespace nebula {
namespace graph {
class ShortestPathExecutor final : public StorageAccessExecutor {
 public:
  ShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ShortestPath", node, qctx) {
    pathNode_ = asNode<ShortestPath>(node);
  }

  folly::Future<Status> execute() override;

 private:
  size_t buildRequestDataSet();

  folly::Future<Status> getNeighbors(size_t rowNum, bool reverse);

  folly::Future<Status> shortestPath(size_t rowNum, size_t stepNum);

  folly::Future<Status> handleResponse(size_t rowNum, size_t stepNum);

  Status handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices);

  Status buildPath(size_t rowNum, RpcResponse&& resp, bool reverse);

  folly::Future<Status> getMeetVidsProps(const std::vector<Value>& meetVids,
                                         std::vector<Value>& meetVertices);

  Status doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  bool conjunctPath(size_t rowNum, size_t stepNum);

  bool buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids);

  void buildOddPath(size_t rowNum, const std::vector<Value>& meetVids);

  std::vector<Row> createRightPath(size_t rowNum, const Value& meetVid, bool evenStep);

  std::vector<Row> createLeftPath(size_t rowNum, const Value& meetVid);

 private:
  const ShortestPath* pathNode_{nullptr};
  size_t maxStep_;
  bool singleShortest_{true};

  std::vector<DataSet> resultDs_;
  std::vector<DataSet> leftVids_;
  std::vector<DataSet> rightVids_;
  std::vector<std::unordered_set<Value>> leftVisitedVids_;
  std::vector<std::unordered_set<Value>> rightVisitedVids_;
  std::vector<std::vector<std::unordered_map<Value, std::vector<Row>>>> allLeftSteps_;
  std::vector<std::vector<std::unordered_map<Value, std::vector<Row>>>> allRightSteps_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
