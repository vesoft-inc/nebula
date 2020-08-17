/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_LISTUSERROLESEXECUTOR_H_
#define EXEC_ADMIN_LISTUSERROLESEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class ListUserRolesExecutor final : public Executor {
public:
    ListUserRolesExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ListUserRolesExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> listUserRoles();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXEC_ADMIN_LISTUSERROLESEXECUTOR_H_
