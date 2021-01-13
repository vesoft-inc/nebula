/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_YIELDEXECUTOR_H_
#define GRAPH_YIELDEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "meta/SchemaProviderIf.h"
#include "graph/AggregateFunction.h"

namespace nebula {
namespace graph {

class YieldExecutor final : public TraverseExecutor {
public:
    YieldExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "YieldExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<InterimResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareYield();

    Status prepareWhere();

    Status syntaxCheck();

    Status beforeExecute();

    Status executeInputs();

    Status getOutputSchema(const InterimResult *inputs, SchemaWriter *outputSchema) const;

    Status executeConstant();

    void finishExecution(std::unique_ptr<RowSetWriter> rsWriter);

    Status checkAggFun();

    Status getAggResultWriter(cpp2::RowValue row, RowSetWriter* rsWriter);

    Status AggregateConstant();

private:
    YieldSentence                              *sentence_;
    std::vector<YieldColumn*>                   yields_;
    bool                                        distinct_{false};
    std::unique_ptr<YieldClauseWrapper>         yieldClauseWrapper_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    Expression                                 *filter_{nullptr};
    std::string                                 varname_;
    std::vector<std::string>                    resultColNames_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    std::vector<nebula::cpp2::SupportedType>    colTypes_;
    std::vector<std::shared_ptr<AggFun>>        aggFuns_;
    bool                                        hasSetResult_{false};
};
}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_YIELDEXECUTOR_H_
