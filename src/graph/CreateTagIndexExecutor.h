/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CREATETAGINDEXEXECUTOR_H
#define GRAPH_CREATETAGINDEXEXECUTOR_H

#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateTagIndexExecutor final : public Executor {
public:
    CreateTagIndexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateTagIndexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    CreateTagIndexSentence       *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CREATETAGINDEXEXECUTOR_H

