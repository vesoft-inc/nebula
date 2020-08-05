/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_ORDERBYEXECUTOR_H_
#define GRAPH_ORDERBYEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class OrderByExecutor final : public TraverseExecutor {
public:
    OrderByExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "OrderByExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    StatusOr<std::unique_ptr<InterimResult>> setupInterimResult();

    Status beforeExecute();

private:
    OrderBySentence                                            *sentence_{nullptr};
    std::vector<std::string>                                    colNames_;
    std::vector<cpp2::RowValue>                                 rows_;
    std::vector<std::pair<int64_t, OrderFactor::OrderType>>     sortFactors_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_ORDERBYEXECUTOR_H_
