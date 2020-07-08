/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXEC_MAINTAIN_ALTERSCHEMAEXECUTOR_H_
#define EXEC_MAINTAIN_ALTERSCHEMAEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {
class AlterTagExecutor final : public Executor {
public:
    AlterTagExecutor(const PlanNode *node, QueryContext *qctx)
            : Executor("AlterTagExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class AlterEdgeExecutor final : public Executor {
public:
    AlterEdgeExecutor(const PlanNode *node, QueryContext *qctx)
            : Executor("AlterEdgeExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MAINTAIN_ALTERSCHEMAEXECUTOR_H_
