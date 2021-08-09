/* Copyright (c) 2021 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXECUTOR_ADMIN_SHOWQUERIESEXECUTOR_H_
#define EXECUTOR_ADMIN_SHOWQUERIESEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class ShowQueriesExecutor final : public Executor {
public:
    ShowQueriesExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowQueriesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    friend class ShowQueriesTest_TestAddQueryAndTopN_Test;
    folly::Future<Status> showCurrentSessionQueries();

    folly::Future<Status> showAllSessionQueries();

    void addQueries(const meta::cpp2::Session& session, DataSet& dataSet) const;
};
}  // namespace graph
}  // namespace nebula
#endif  // EXECUTOR_ADMIN_SHOWQUERIESEXECUTOR_H_
