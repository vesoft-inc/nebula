/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_CREATEUSEREXECUTOR_H_
#define EXEC_ADMIN_CREATEUSEREXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class CreateUserExecutor final : public Executor {
public:
    CreateUserExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("CreateUserExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> createUser();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXEC_ADMIN_CREATEUSEREXECUTOR_H_
