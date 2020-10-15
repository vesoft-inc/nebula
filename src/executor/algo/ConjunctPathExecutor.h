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

private:
    folly::Future<Status> bfsShortestPath();

    std::vector<Row> findBfsShortestPath(Iterator* iter,
                                         bool isLatest,
                                         std::multimap<Value, const Edge*>& table);

    std::multimap<Value, Path> buildBfsInterimPath(
        std::unordered_set<Value>& meets,
        std::vector<std::multimap<Value, const Edge*>>& hist);

    folly::Future<Status> conjunctPath();

    bool findPath(Iterator* iter, std::multimap<Value, const Path*>& table, DataSet& ds);

    std::vector<std::multimap<Value, const Edge*>>  forward_;
    std::vector<std::multimap<Value, const Edge*>>  backward_;
};
}  // namespace graph
}  // namespace nebula
#endif  // EXECUTOR_ALGO_CONJUNCTPATHEXECUTOR_H_
