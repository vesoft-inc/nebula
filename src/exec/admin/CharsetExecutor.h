/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXEC_ADMIN_CHARSETEXECUTOR_H_
#define EXEC_ADMIN_CHARSETEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class ShowCharsetExecutor final : public Executor {
public:
    ShowCharsetExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ShowCharsetExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class ShowCollationExecutor final : public Executor {
public:
    ShowCollationExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ShowCollationExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};
}   // namespace graph
}   // namespace nebula

#endif   // EXEC_ADMIN_CHARSETEXECUTOR_H_
