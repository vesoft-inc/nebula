/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_DESCRIBEEDGEEXECUTOR_H_
#define GRAPH_DESCRIBEEDGEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DescribeEdgeExecutor final : public Executor {
public:
    DescribeEdgeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DescribeEdgeExecutor";
    }

    Status VE_MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    DescribeEdgeSentence                       *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DESCRIBEEDGEEXECUTOR_H_
