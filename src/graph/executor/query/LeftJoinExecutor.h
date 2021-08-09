/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_LEFTJOINEXECUTOR_H_
#define EXECUTOR_QUERY_LEFTJOINEXECUTOR_H_

#include "executor/query/JoinExecutor.h"

namespace nebula {
namespace graph {

class LeftJoinExecutor final : public JoinExecutor {
public:
    LeftJoinExecutor(const PlanNode *node, QueryContext *qctx)
        : JoinExecutor("LeftJoinExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

    Status close() override;

private:
    folly::Future<Status> join();

    DataSet probe(const std::vector<Expression*>& probeKeys,
                  Iterator* probeIter,
                  const std::unordered_map<List, std::vector<const Row*>>& hashTable) const;

    DataSet singleKeyProbe(
        Expression* probeKey,
        Iterator* probeIter,
        const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const;

    template <class T>
    void buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                     const T& val,
                     const Row& lRow,
                     DataSet& ds) const;

private:
    size_t rightColSize_{0};
};
}  // namespace graph
}  // namespace nebula
#endif
