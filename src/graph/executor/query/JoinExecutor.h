/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_JOINEXECUTOR_H_
#define EXECUTOR_QUERY_JOINEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class JoinExecutor : public Executor {
public:
    JoinExecutor(const std::string& name, const PlanNode* node, QueryContext* qctx)
        : Executor(name, node, qctx) {}

    Status checkInputDataSets();

    void buildHashTable(const std::vector<Expression*>& hashKeys, Iterator* iter);

protected:
    void buildHashTable(const std::vector<Expression*>& hashKeys,
                        Iterator* iter,
                        std::unordered_map<List, std::vector<const Row*>>& hashTable) const;

    void buildSingleKeyHashTable(
        Expression* hashKey,
        Iterator* iter,
        std::unordered_map<Value, std::vector<const Row*>>& hashTable) const;

    std::unique_ptr<Iterator>                          lhsIter_;
    std::unique_ptr<Iterator>                          rhsIter_;
    size_t                                             colSize_{0};
};
}  // namespace graph
}  // namespace nebula
#endif
