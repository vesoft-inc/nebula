/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_SEQUENTIALEXECUTOR_H_
#define GRAPH_SEQUENTIALEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"


namespace nebula {
namespace graph {

class SequentialExecutor final : public Executor {
public:
    SequentialExecutor(SequentialSentences *sentences, ExecutionContext *ectx);

    const char* name() const override {
        return "SequentialExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    SequentialSentences                        *sentences_{nullptr};
    std::vector<std::unique_ptr<Executor>>      executors_;
};


}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_SEQUENTIALEXECUTOR_H_
