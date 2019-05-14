/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_CREATETAGEXECUTOR_H_
#define GRAPH_CREATETAGEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateTagExecutor final : public Executor {
public:
    CreateTagExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateTagExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    CreateTagSentence                          *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_DEFINETAGEXECUTOR_H_
