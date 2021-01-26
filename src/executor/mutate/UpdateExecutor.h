/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_MUTATE_UPDATEEXECUTOR_H_
#define EXECUTOR_MUTATE_UPDATEEXECUTOR_H_

#include "common/base/StatusOr.h"
#include "executor/StorageAccessExecutor.h"

namespace nebula {
namespace graph {

class UpdateBaseExecutor : public StorageAccessExecutor {
public:
    UpdateBaseExecutor(const std::string &execName,
                       const PlanNode *node,
                       QueryContext *qctx)
        : StorageAccessExecutor(execName, node, qctx) {}

    virtual ~UpdateBaseExecutor() {}

protected:
    StatusOr<DataSet> handleResult(DataSet &&data);

protected:
    std::vector<std::string>         yieldNames_;
};

class UpdateVertexExecutor final : public UpdateBaseExecutor {
public:
    UpdateVertexExecutor(const PlanNode *node, QueryContext *qctx)
        : UpdateBaseExecutor("UpdateVertexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class UpdateEdgeExecutor final : public UpdateBaseExecutor {
public:
    UpdateEdgeExecutor(const PlanNode *node, QueryContext *qctx)
        : UpdateBaseExecutor("UpdateEdgeExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_MUTATE_UPDATEEXECUTOR_H_
