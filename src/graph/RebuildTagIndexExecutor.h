/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_REBUILDTAGINDEXEXECUTOR_H_
#define GRAPH_REBUILDTAGINDEXEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class RebuildTagIndexExecutor final : public Executor {
public:
    RebuildTagIndexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "RebuildTagIndexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    RebuildTagIndexSentence                          *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_REBUILDTAGINDEXEXECUTOR_H_

