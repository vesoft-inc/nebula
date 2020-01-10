/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_BUILDTAGINDEXEXECUTOR_H_
#define GRAPH_BUILDTAGINDEXEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class BuildTagIndexExecutor final : public Executor {
public:
    BuildTagIndexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "BuildTagIndexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    BuildTagIndexSentence                          *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_BUILDTAGINDEXEXECUTOR_H_

