/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_GOEXECUTOR_H_
#define GRAPH_GOEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class GoExecutor final : public TraverseExecutor {
public:
    GoExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GoExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(TraverseRecords records) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareResultSchema();

    Status prepareStep();

    Status prepareFrom();

    Status prepareOver();

    Status prepareWhere();

    Status prepareYield();

    Status prepareNeededProps();

    void cacheResult(TraverseRecords records);

private:
    GoSentence                                 *sentence_{nullptr};
    uint32_t                                    steps_{1};
    bool                                        upto_{false};
    bool                                        reversely_{false};
    std::string                                *edge_;
    Expression                                 *filter_{nullptr};
    std::vector<YieldColumn*>                   yields_;
    TraverseRecords                             inputs_;
    std::unique_ptr<ExpressionContext>          expctx_;
    std::vector<int64_t>                        starts_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_GOEXECUTOR_H_
