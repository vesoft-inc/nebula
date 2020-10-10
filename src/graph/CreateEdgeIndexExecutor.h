/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CREATEEDGEINDEXEXECUTOR_H
#define GRAPH_CREATEEDGEINDEXEXECUTOR_H

#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateEdgeIndexExecutor final : public Executor {
public:
    CreateEdgeIndexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateEdgeIndexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    CreateEdgeIndexSentence       *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CREATEEDGEINDEXEXECUTOR_H

