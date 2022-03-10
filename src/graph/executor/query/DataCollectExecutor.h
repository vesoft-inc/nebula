/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_DATACOLLECTEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_DATACOLLECTEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

//
// Member:
//  `colNames_` : save the column name of the result of the DataCollect
// Funcitons:
//  `collectSubgraph` : receive result from GetNeighbors, collect vertices & edges by calling
//   GetNeighborIter's getVertices & getEdges interface
//  `rowBaseMove` :
//  `collectMToN` : Only used in go  MToN scenarios.
//  `collectBFSShortest` : `collectAllPaths` : `collectMultiplePairShortestPath`
//  : `collectPathProp` :
//  `collectAllPaths`
//

class DataCollectExecutor final : public Executor {
 public:
  DataCollectExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("DataCollectExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> doCollect();

  Status collectSubgraph(const std::vector<std::string>& vars);

  Status rowBasedMove(const std::vector<std::string>& vars);

  Status collectMToN(const std::vector<std::string>& vars, const StepClause& mToN, bool distinct);

  Status collectAllPaths(const std::vector<std::string>& vars);

  Status collectPathProp(const std::vector<std::string>& vars);

  std::vector<std::string> colNames_;
  Value result_;
};
}  // namespace graph
}  // namespace nebula
#endif
