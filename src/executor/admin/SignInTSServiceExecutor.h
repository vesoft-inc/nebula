/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_SIGNINTSSERVICEEXECUTOR_H_
#define EXECUTOR_ADMIN_SIGNINTSSERVICEEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class SignInTSServiceExecutor final : public Executor {
public:
    SignInTSServiceExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("SignInTSServiceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> signInTSService();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_SIGNINTSSERVICEEXECUTOR_H_
