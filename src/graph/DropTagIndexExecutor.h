/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DROPTAGINDEXEXECUTOR_H
#define GRAPH_DROPTAGINDEXEXECUTOR_H

#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DropTagIndexExecutor final : public Executor {
public:
    DropTagIndexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DropTagIndexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DropTagIndexSentence                       *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DROPTAGINDEXEXECUTOR_H

