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
    using JobOpEnum = nebula::meta::cpp2::AdminJobOp;
    using TJobResult = nebula::meta::cpp2::AdminJobResult;

public:
    AdminJobExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AdminJobExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    std::vector<std::string>
    getHeader(JobOpEnum op, bool succeed = true);
    bool opNeedsSpace(JobOpEnum op);
    JobOpEnum toAdminJobOp(const std::string& op);
    std::vector<cpp2::RowValue> toRowValues(JobOpEnum op, TJobResult &&resp);
    cpp2::RowValue toRowValue(const nebula::meta::cpp2::JobDesc&);
    cpp2::RowValue toRowValue(const nebula::meta::cpp2::TaskDesc&);
    cpp2::RowValue toRowValue(std::string&& msg);
    static std::string time2string(int64_t);
    static std::string toString(nebula::meta::cpp2::JobStatus st);
    static std::string toString(nebula::cpp2::HostAddr st);

private:
    AdminSentence                             *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_AdminJobExecutor_H
