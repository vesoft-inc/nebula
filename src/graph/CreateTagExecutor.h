/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CREATETAGEXECUTOR_H_
#define GRAPH_CREATETAGEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateTagExecutor final : public Executor {
public:
    CreateTagExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateTagExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status getSchema();

private:
    CreateTagSentence                          *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>         exprCtx_{nullptr};
    nebula::cpp2::Schema                        schema_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_DEFINETAGEXECUTOR_H_
