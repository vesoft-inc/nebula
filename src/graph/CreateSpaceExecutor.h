/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    CreateSpaceSentence     *sentence_{nullptr};
    const std::string       *spaceName_{nullptr};
    int32_t                  partNum_{0};
    int32_t                  replicaFactor_{0};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CREATESPACEEXECUTOR_H_
