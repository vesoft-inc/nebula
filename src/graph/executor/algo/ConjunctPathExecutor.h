/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ALGO_CONJUNCTPATHEXECUTOR_H_
#define EXECUTOR_ALGO_CONJUNCTPATHEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
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
#endif  // EXECUTOR_ALGO_CONJUNCTPATHEXECUTOR_H_
