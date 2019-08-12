/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SCHEMA_DROPTAGEXECUTOR_H_
#define GRAPH_SCHEMA_DROPTAGEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DropTagExecutor final : public Executor {
public:
    DropTagExecutor(Sentence *sentence, ExecutionContext *context);

    const char* name() const override {
        return "DropTagExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DropTagSentence *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_SCHEMA_DROPTAGEXECUTOR_H_
