/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DROPSPACEEXECUTOR_H_
#define GRAPH_DROPSPACEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DropSpaceExecutor final : public Executor {
public:
    DropSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DropSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DropSpaceSentence       *sentence_{nullptr};
    const std::string       *spaceName_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DROPSPACEEXECUTOR_H_
