/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SEQUENTIALEXECUTOR_H_
#define GRAPH_SEQUENTIALEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "graph/GraphFlags.h"
#include "graph/PermissionCheck.h"

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
    void executeSub(unsigned idx);

private:
    SequentialSentences                        *sentences_{nullptr};
    std::vector<std::unique_ptr<Executor>>      executors_;
    uint32_t                                    respExecutorIndex_{0};
};


}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_SEQUENTIALEXECUTOR_H_
