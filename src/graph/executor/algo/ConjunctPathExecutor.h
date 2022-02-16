/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ALGO_CONJUNCTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_CONJUNCTPATHEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future
#include <stddef.h>                // for size_t

#include <string>         // for allocator, string
#include <unordered_map>  // for unordered_map, unordered_...
#include <unordered_set>  // for unordered_set
#include <vector>         // for vector

#include "common/base/Status.h"        // for Status
#include "common/datatypes/DataSet.h"  // for DataSet (ptr only), Row
#include "common/datatypes/Value.h"    // for Value, hash, operator==
#include "graph/executor/Executor.h"   // for Executor

namespace nebula {
namespace graph {
class Iterator;
class PlanNode;
class QueryContext;
}  // namespace graph
struct Edge;
struct List;
struct Path;

struct Edge;
struct List;
struct Path;

namespace graph {
class Iterator;
class PlanNode;
class QueryContext;

class ConjunctPathExecutor final : public Executor {
 public:
  ConjunctPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("ConjunctPathExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

  struct CostPaths {
    CostPaths(Value& cost, const List& paths) : cost_(cost), paths_(paths) {}
    Value cost_;
    const List& paths_;
  };

 private:
  using CostPathsValMap = std::unordered_map<Value, std::unordered_map<Value, CostPaths>>;

  folly::Future<Status> bfsShortestPath();

  folly::Future<Status> allPaths();

  std::vector<Row> findBfsShortestPath(Iterator* iter,
                                       bool isLatest,
                                       std::unordered_multimap<Value, const Edge*>& table);

  std::unordered_multimap<Value, Path> buildBfsInterimPath(
      std::unordered_set<Value>& meets,
      std::vector<std::unordered_multimap<Value, const Edge*>>& hist);

  folly::Future<Status> floydShortestPath();

  bool findPath(Iterator* backwardPathIter, CostPathsValMap& forwardPathtable, DataSet& ds);

  Status conjunctPath(const List& forwardPaths,
                      const List& backwardPaths,
                      Value& cost,
                      DataSet& ds);

  bool findAllPaths(Iterator* backwardPathsIter,
                    std::unordered_map<Value, const List&>& forwardPathsTable,
                    DataSet& ds);
  void delPathFromConditionalVar(const Value& start, const Value& end);

 private:
  std::vector<std::unordered_multimap<Value, const Edge*>> forward_;
  std::vector<std::unordered_multimap<Value, const Edge*>> backward_;
  size_t count_{0};
  // startVid : {endVid, cost}
  std::unordered_map<Value, std::unordered_map<Value, Value>> historyCostMap_;
  std::string conditionalVar_;
  bool noLoop_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_CONJUNCTPATHEXECUTOR_H_
