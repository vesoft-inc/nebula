/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/logic/SelectExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {

SelectExecutor::SelectExecutor(const PlanNode* node,
                               ExecutionContext* ectx,
                               Executor* then,
                               Executor* els)
    : Executor("SelectExecutor", node, ectx),
      then_(DCHECK_NOTNULL(then)),
      else_(DCHECK_NOTNULL(els)) {}

folly::Future<Status> SelectExecutor::execute() {
    dumpLog();

    auto* select = asNode<Selector>(node());
    auto* expr = select->condition();
    UNUSED(expr);

    finish(nebula::Value(true));

    // FIXME: store expression value to execution context
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
