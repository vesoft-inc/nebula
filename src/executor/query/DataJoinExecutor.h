/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_DATAJOINEXECUTOR_H_
#define EXECUTOR_QUERY_DATAJOINEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class DataJoinExecutor final : public Executor {
public:
    DataJoinExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DataJoinExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

    Status close() override;

private:
    folly::Future<Status> doInnerJoin();

    void buildHashTable(const std::vector<Expression*>& hashKeys, Iterator* iter);

    void probe(const std::vector<Expression*>& probeKeys, Iterator* probeiter,
               JoinIter* resultIter);

private:
    bool                                                      exchange_{false};
    std::unordered_map<List, std::vector<const LogicalRow*>>  hashTable_;
};
}  // namespace graph
}  // namespace nebula
#endif
