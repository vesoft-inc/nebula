/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_DOWNLOADEXECUTOR_H_
#define EXECUTOR_ADMIN_DOWNLOADEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class DownloadExecutor final : public Executor {
public:
    DownloadExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DownloadExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_DOWNLOADEXECUTOR_H_
