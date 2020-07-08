/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_LIMITEXECUTOR_H_
#define EXEC_QUERY_LIMITEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class LimitExecutor final : public Executor {
public:
    LimitExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("LimitExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_LIMITEXECUTOR_H_
