/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTIONPLAN_H_
#define GRAPH_EXECUTIONPLAN_H_

#include "base/Base.h"
#include "base/Status.h"
#include "cpp/helpers.h"
#include "parser/GQLParser.h"
#include "graph/ExecutionContext.h"
#include "graph/SequentialExecutor.h"

/**
 * ExecutionPlan coordinates the execution process,
 * i.e. parse a query into a parsing tree, analyze the tree,
 * initiate and finalize the execution.
 */

namespace nebula {
namespace graph {

class ExecutionPlan final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    explicit ExecutionPlan(std::unique_ptr<ExecutionContext> ectx) {
        ectx_ = std::move(ectx);
        allStats_ = std::make_unique<stats::Stats>("graph", "all");
        parseStats_ = std::make_unique<stats::Stats>("graph", "parse");
        slowQueryStats_ = std::make_unique<stats::Stats>("graph", "slow_query");
    }

    ~ExecutionPlan() = default;

    void execute();

    /**
     * If the whole execution was done, `onFinish' would be invoked.
     * All `onFinish' should do is to ask `executor_' to fill the `ectx()->rctx()->resp()',
     * which in turn asks the last sub-executor to do the actual job.
     */
    void onFinish();

    /**
     * If any error occurred during the execution, `onError' should
     * be invoked with a status to indicate the reason.
     */
    void onError(Status);

    ExecutionContext* ectx() const {
        return ectx_.get();
    }

private:
    std::unique_ptr<SequentialSentences>        sentences_;
    std::unique_ptr<ExecutionContext>           ectx_;
    std::unique_ptr<SequentialExecutor>         executor_;
    std::unique_ptr<stats::Stats>               allStats_;
    std::unique_ptr<stats::Stats>               parseStats_;
    std::unique_ptr<stats::Stats>               slowQueryStats_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTIONPLAN_H_
