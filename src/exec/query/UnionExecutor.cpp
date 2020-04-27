/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/UnionExecutor.h"

#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

UnionExecutor::UnionExecutor(const PlanNode *node,
                             ExecutionContext *ectx,
                             Executor *left,
                             Executor *right)
    : MultiInputsExecutor("UnionExecutor", node, ectx, {left, right}) {
    DCHECK_NOTNULL(left);
    DCHECK_NOTNULL(right);
}

folly::Future<Status> UnionExecutor::execute() {
    return MultiInputsExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));
        dumpLog();
        // TODO(yee): implement union results of left and right inputs
        return start();
    }));
}

}   // namespace graph
}   // namespace nebula
