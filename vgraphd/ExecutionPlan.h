/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_EXECUTIONPLAN_H_
#define VGRAPHD_EXECUTIONPLAN_H_

#include "base/Base.h"
#include "base/Status.h"
#include "cpp/helpers.h"
#include "parser/GQLParser.h"
#include "vgraphd/ExecutionContext.h"
#include "vgraphd/CompoundExecutor.h"

/**
 * ExecutionPlan coordinates the execution process,
 * i.e. parse a query into a parsing tree, analyze the tree,
 * initiate and finalize the execution.
 */

namespace vesoft {
namespace vgraph {

class ExecutionPlan final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    explicit ExecutionPlan(std::unique_ptr<ExecutionContext> ectx) {
        ectx_ = std::move(ectx);
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
    std::unique_ptr<CompoundSentence>           compound_;
    std::unique_ptr<ExecutionContext>           ectx_;
    std::unique_ptr<CompoundExecutor>           executor_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_EXECUTIONPLAN_H_
