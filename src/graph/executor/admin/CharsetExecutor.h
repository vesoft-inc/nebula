/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXECUTOR_ADMIN_CHARSETEXECUTOR_H_
#define EXECUTOR_ADMIN_CHARSETEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class ShowCharsetExecutor final : public Executor {
public:
    ShowCharsetExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowCharsetExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ShowCollationExecutor final : public Executor {
public:
    ShowCollationExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowCollationExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};
}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_CHARSETEXECUTOR_H_
