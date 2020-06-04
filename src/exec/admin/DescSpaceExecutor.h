/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_DESCSPACEEXECUTOR_H_
#define EXEC_ADMIN_DESCSPACEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class DescSpaceExecutor final : public Executor {
public:
    DescSpaceExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DescSpaceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> descSpace();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_ADMIN_DESCSPACEEXECUTOR_H_
