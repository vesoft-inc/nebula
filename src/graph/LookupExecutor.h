/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_FINDEXECUTOR_H_
#define GRAPH_FINDEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

using OperatorList = std::vector<std::pair<std::pair<std::string, VariantType>,
                                           RelationalExpression::Operator>>;

class LookupExecutor final : public TraverseExecutor {
public:
    LookupExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "LookupExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void feedResult(std::unique_ptr<InterimResult> result) override {
        UNUSED(result);
    }

    void execute() override;

private:
    /**
     * To do some preparing works on the clauses
     */
    Status prepareClauses();

    Status prepareYieldClause();

    Status prepareFrom();

    Status prepareWhere();

    Status setupIndexHint(bool isRange);

    StatusOr<nebula::cpp2::IndexHintItem>
    prepareIndexHint(VariantType v,
                     nebula::cpp2::SupportedType t,
                     bool isEqual,
                     bool isGreater);

    StatusOr<nebula::cpp2::IndexHintItem>
    getIndexHintItem(VariantType v,
                     RelationalExpression::Operator op,
                     nebula::cpp2::SupportedType t);

    StatusOr<nebula::cpp2::IndexHintItem>
    getIndexHintItem(nebula::cpp2::SupportedType t);

    StatusOr<std::string> getValue(VariantType v, nebula::cpp2::SupportedType t);

    StatusOr<std::string> prepareAPE(const Expression* expr);

    StatusOr<VariantType> preparePE(const Expression* expr);

    Status prepareRE(const RelationalExpression* expr);

    StatusOr<RelationalExpression::Operator> reverseOperator(RelationalExpression::Operator op);

    Status prepareLE(const LogicalExpression* expr);

    Status prepareExpression(const Expression* expr);

    void scanIndex();

private:
    bool                                       isEdge_{false};
    GraphSpaceID                               spaceId_;
    IndexID                                    indexId_;
    const std::string                          *from_{nullptr};
    LookupSentence                             *sentence_{nullptr};
    std::vector<nebula::cpp2::ColumnDef>       fields_;
    std::vector<nebula::cpp2::ColumnDef>       returnCols_;
    nebula::cpp2::IndexHint                    indexHint_;
    OperatorList                               operatorList_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_FINDEXECUTOR_H_
