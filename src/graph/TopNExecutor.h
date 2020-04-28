/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TOPNEXECUTOR_H_
#define GRAPH_TOPNEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {
class TopNExecutor final : public TraverseExecutor {
public:
    TopNExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "TopNExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    StatusOr<std::unique_ptr<InterimResult>> setupInterimResult();

private:
    TopNSentence                                               *sentence_{nullptr};
    std::vector<std::string>                                    colNames_;
    std::vector<cpp2::RowValue>                                 rows_;
    std::vector<int64_t>                                        indexes_;
};
}  // namespace graph
}  // namespace nebula
#endif
