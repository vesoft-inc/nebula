/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_ADMINJOBEXECUTOR_H
#define GRAPH_ADMINJOBEXECUTOR_H

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class AdminJobExecutor final : public Executor {
public:
    AdminJobExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AdminJobExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    // void executeHttp();

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    std::vector<std::string>
    getHeader(nebula::meta::cpp2::AdminJobOp op, bool succeed = true);
    bool opNeedsSpace(nebula::meta::cpp2::AdminJobOp op);
    nebula::meta::cpp2::AdminJobOp toAdminJobOp(const std::string& op);

private:
    AdminSentence                             *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_AdminJobExecutor_H
