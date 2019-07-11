/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_INGESTEXECUTOR_H
#define GRAPH_INGESTEXECUTOR_H

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class IngestExecutor final : public Executor {
public:
    IngestExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "IngestExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    IngestSentence        *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INGESTEXECUTOR_H

