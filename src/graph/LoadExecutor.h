/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_LOADEXECUTOR_H_
#define GRAPH_LOADEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class LoadExecutor final : public Executor {
public:
    LoadExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "LoadExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    LoadSentence *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse> resp_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_LOADEXECUTOR_H_
