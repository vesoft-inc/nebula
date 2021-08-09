/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_TOPNEXECUTOR_H_
#define EXECUTOR_QUERY_TOPNEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class TopNExecutor final : public Executor {
public:
    TopNExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("TopNExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    template<typename U>
    void executeTopN(Iterator *iter);

    int64_t offset_;
    int64_t maxCount_;
    int64_t heapSize_;
    std::function<bool(const Row&, const Row&)> comparator_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_QUERY_TOPNEXECUTOR_H_
