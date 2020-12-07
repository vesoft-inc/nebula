/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_BALANCEEXECUTOR_H_
#define GRAPH_BALANCEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class BalanceExecutor final : public Executor {
public:
    BalanceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "BalanceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void balanceLeader();

    void balanceData(bool isStop, bool isReset);

    void stopBalanceData();

    void showBalancePlan();

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    BalanceSentence                          *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>  resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_BALANCEEXECUTOR_H_
