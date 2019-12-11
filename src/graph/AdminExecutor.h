/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_ADMINEXECUTOR_H
#define GRAPH_ADMINEXECUTOR_H

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class AdminExecutor final : public Executor {
public:
    AdminExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AdminExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    bool opNeedsSpace(const std::string& op);

private:
    AdminSentence                             *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    std::map<std::string, std::string>          header_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_ADMINEXECUTOR_H
