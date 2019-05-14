/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_REMOVEEDGEEXECUTOR_H
#define GRAPH_REMOVEEDGEEXECUTOR_H

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class RemoveEdgeExecutor final : public Executor {
public:
    RemoveEdgeExecutor(Sentence *sentence, ExecutionContext *context);

    const char* name() const override {
        return "RemoveEdgeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    RemoveEdgeSentence *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_REMOVEEDGEEXECUTOR_H

