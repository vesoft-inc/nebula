/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_DATACOLLECTEXECUTOR_H_
#define EXECUTOR_QUERY_DATACOLLECTEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class DataCollectExecutor final : public Executor {
public:
    DataCollectExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DataCollectExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> doCollect();

    Status collectSubgraph(const std::vector<std::string>& vars);

    Status rowBasedMove(const std::vector<std::string>& vars);

    Status collectMToN(const std::vector<std::string>& vars, const StepClause& mToN, bool distinct);

    Status collectBFSShortest(const std::vector<std::string>& vars);

    Status collectAllPaths(const std::vector<std::string>& vars);

    Status collectMultiplePairShortestPath(const std::vector<std::string>& vars);

    Status collectPathProp(const std::vector<std::string>& vars);

    std::vector<std::string>    colNames_;
    Value                       result_;
};
}  // namespace graph
}  // namespace nebula
#endif
