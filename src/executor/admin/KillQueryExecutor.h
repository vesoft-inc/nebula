/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_KILLQUERYEXECUTOR_H_
#define EXECUTOR_ADMIN_KILLQUERYEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class KillQueryExecutor final : public Executor {
public:
    KillQueryExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("KillQueryExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    using QueriesMap = std::unordered_map<SessionID, std::unordered_set<ExecutionPlanID>>;
    Status verifyTheQueriesByLocalCache(QueriesMap& toBeVerifiedQueries,
                                        QueriesMap& killQueries);

    Status verifyTheQueriesByMetaInfo(const QueriesMap& toBeVerifiedQueries,
                                      const std::vector<meta::cpp2::Session>& sessionsInMeta);

    void killCurrentHostQueries(const QueriesMap& killQueries, QueriesMap& queriesKilledInLocal);

    void findKillQueriesViaMeta(const QueriesMap& queriesKilledInLocal,
                                const std::vector<meta::cpp2::Session>& sessionsInMeta,
                                QueriesMap& killQueries);
};
}  // namespace graph
}  // namespace nebula

#endif  // EXECUTOR_ADMIN_KILLQUERYEXECUTOR_H_
