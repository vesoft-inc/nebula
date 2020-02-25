/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CREATEEDGEEXECUTOR_H_
#define GRAPH_CREATEEDGEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateEdgeExecutor final : public Executor {
public:
    CreateEdgeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateEdgeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status getSchema();

private:
    CreateEdgeSentence                         *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>         exprCtx_{nullptr};
    nebula::cpp2::Schema                        schema_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DEFINEEDGEEXECUTOR_H_
