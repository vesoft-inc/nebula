/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_
#define EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class SubmitJobExecutor final : public Executor {
public:
    SubmitJobExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("SubmitJobExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_
