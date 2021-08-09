/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_SHOW_HOSTS_EXECUTOR_H_
#define EXECUTOR_ADMIN_SHOW_HOSTS_EXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class ShowHostsExecutor final : public Executor {
public:
    ShowHostsExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowHostsExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> showHosts();
};

}  // namespace graph
}  // namespace nebula

#endif
