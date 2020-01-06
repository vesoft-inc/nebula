/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DESCRIBEEDGEINDEXEXECUTOR_H
#define GRAPH_DESCRIBEEDGEINDEXEXECUTOR_H

#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DescribeEdgeIndexExecutor final : public Executor {
public:
    DescribeEdgeIndexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DescribeEdgeIndexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    DescribeEdgeIndexSentence                  *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DESCRIBEEDGEINDEXEXECUTOR_H

