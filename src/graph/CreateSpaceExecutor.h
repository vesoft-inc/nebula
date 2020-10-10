/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CREATESPACEEXECUTOR_H_
#define GRAPH_CREATESPACEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateSpaceExecutor final : public Executor {
public:
    CreateSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    CreateSpaceSentence            *sentence_{nullptr};
    // TODO Due to the currently design of the createSpace interface,
    // it's impossible to express *not specified*, so we use 0 to
    // indicate partNum_ and replicaFactor_ in spaceDesc_.
    meta::SpaceDesc                spaceDesc_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CREATESPACEEXECUTOR_H_
