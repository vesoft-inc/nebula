/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_LISTENEREXECUTOR_H_
#define EXECUTOR_ADMIN_LISTENEREXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class AddListenerExecutor final : public Executor {
public:
    AddListenerExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("AddListenerExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class RemoveListenerExecutor final : public Executor {
public:
    RemoveListenerExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("RemoveListenerExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ShowListenerExecutor final : public Executor {
public:
    ShowListenerExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowListenerExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_LISTENEREXECUTOR_H_
