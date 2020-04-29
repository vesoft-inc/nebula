/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_LIMITEXECUTOR_H_
#define GRAPH_LIMITEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class LimitExecutor final : public TraverseExecutor {
public:
    LimitExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "LimitExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    StatusOr<std::unique_ptr<InterimResult>> setupInterimResult();

    void onEmptyInputs();

private:
    LimitSentence                                            *sentence_{nullptr};
    std::vector<std::string>                                  colNames_;
    std::vector<cpp2::RowValue>                               rows_;
    int64_t                                                   offset_{-1};
    int64_t                                                   count_{-1};
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_LIMITEXECUTOR_H
