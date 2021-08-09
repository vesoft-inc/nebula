/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ALGO_PRODUCEALLPATHSEXECUTOR_H_
#define EXECUTOR_ALGO_PRODUCEALLPATHSEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class ProduceAllPathsExecutor final : public Executor {
public:
    ProduceAllPathsExecutor(const PlanNode* node, QueryContext* qctx)
        : Executor("ProduceAllPaths", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    // k: dst, v: paths to dst
    using HistoryPaths = std::unordered_map<Value, std::vector<const Path*>>;

    // k: dst, v: paths to dst
    using Interims = std::unordered_map<Value, std::vector<Value>>;

    void createPaths(const Edge& edge, Interims& interims);

    void buildPaths(const std::vector<const Path*>& history, const Edge& edge, Interims& interims);

    size_t count_{0};
    HistoryPaths historyPaths_;
    bool noLoop_{false};
};
}  // namespace graph
}  // namespace nebula
#endif
