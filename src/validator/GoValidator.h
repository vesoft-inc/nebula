/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GOVALIDATOR_H_
#define VALIDATOR_GOVALIDATOR_H_

#include "planner/plan/Query.h"
#include "validator/TraversalValidator.h"
#include "context/ast/QueryAstContext.h"

namespace nebula {
namespace graph {
class GoValidator final : public TraversalValidator {
public:
    using VertexProp = nebula::storage::cpp2::VertexProp;
    using EdgeProp = nebula::storage::cpp2::EdgeProp;

    GoValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    AstContext* getAstContext() override {
        return goCtx_.get();
    }

    Status validateWhere(WhereClause* where);

    Status validateYield(YieldClause* yield);

    Status validateTruncate(TruncateClause* truncate);

    Status buildColumns();

    void extractPropExprs(const Expression* expr);

    Expression* rewrite2VarProp(const Expression* expr);

private:
    std::unique_ptr<GoContext>                      goCtx_;

    YieldColumns*                                   inputPropCols_{nullptr};
    std::unordered_map<std::string, YieldColumn*>   propExprColMap_;
};
}   // namespace graph
}   // namespace nebula
#endif
