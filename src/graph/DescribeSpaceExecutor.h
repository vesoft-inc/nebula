/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DESCRIBESPACEEXECUTOR_H_
#define GRAPH_DESCRIBESPACEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "graph/GraphFlags.h"
#include "common/permission//PermissionManager.h"

namespace nebula {
namespace graph {

class DescribeSpaceExecutor final : public Executor {
public:
    DescribeSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DescribeSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    DescribeSpaceSentence                      *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DESCRIBESPACEEXECUTOR_H_
