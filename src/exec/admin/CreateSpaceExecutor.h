/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_CREATESPACEEXECUTOR_H_
#define EXEC_ADMIN_CREATESPACEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class CreateSpaceExecutor final : public Executor {
public:
    CreateSpaceExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("CreateSpaceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> createSpace();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_ADMIN_CREATESPACEEXECUTOR_H_
