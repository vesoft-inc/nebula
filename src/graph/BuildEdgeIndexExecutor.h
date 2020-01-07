/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_BUILDEDGEINDEXEXECUTOR_H_
#define GRAPH_BUILDEDGEINDEXEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class BuildEdgeIndexExecutor final : public Executor {
public:
    BuildEdgeIndexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "BuildEdgeIndexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    BuildEdgeIndexSentence                          *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_BUILDEDGEINDEXEXECUTOR_H_
