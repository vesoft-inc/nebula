/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_DEFINETAGEXECUTOR_H_
#define GRAPH_DEFINETAGEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DefineTagExecutor final : public Executor {
public:
    DefineTagExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DefineTagExecutor";
    }

    Status VE_MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DefineTagSentence                          *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_DEFINETAGEXECUTOR_H_
