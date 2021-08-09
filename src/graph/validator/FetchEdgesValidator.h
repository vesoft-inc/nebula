/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _VALIDATOR_FETCH_EDGES_VALIDATOR_H_
#define _VALIDATOR_FETCH_EDGES_VALIDATOR_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/TraverseSentences.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class FetchEdgesValidator final : public Validator {
public:
    using EdgeProp = nebula::storage::cpp2::EdgeProp;
    using Expr = nebula::storage::cpp2::Expr;
    FetchEdgesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareEdges();

    Status preparePropertiesWithYield(const YieldClause* yield);
    Status preparePropertiesWithoutYield();
    Status prepareProperties();

    static const Expression* findInvalidYieldExpression(const Expression* root);

    // TODO(shylock) merge the code
    std::string buildConstantInput();
    std::string buildRuntimeInput();

    Expression* notEmpty(Expression* expr) {
        return RelationalExpression::makeNE(
            qctx_->objPool(),
            ConstantExpression::make(qctx_->objPool(), Value::kEmpty),
            DCHECK_NOTNULL(expr));
    }

    Expression* lgAnd(Expression* left, Expression* right) {
        return LogicalExpression::makeAnd(
            qctx_->objPool(), DCHECK_NOTNULL(left), DCHECK_NOTNULL(right));
    }

    Expression* emptyEdgeKeyFilter();

    static const std::unordered_set<std::string> reservedProperties;

private:
    GraphSpaceID spaceId_;
    DataSet edgeKeys_{{kSrc, kRank, kDst}};
    Expression* srcRef_{nullptr};
    Expression* rankRef_{nullptr};
    Expression* dstRef_{nullptr};
    Expression* src_{nullptr};
    Expression* type_{nullptr};
    Expression* rank_{nullptr};
    Expression* dst_{nullptr};
    std::string edgeTypeName_;
    EdgeType edgeType_{0};
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::unique_ptr<std::vector<EdgeProp>> props_;
    std::unique_ptr<std::vector<Expr>>     exprs_;
    bool dedup_{false};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::vector<storage::cpp2::OrderBy> orderBy_{};
    std::string filter_{""};
    // valid when yield expression not require storage
    // So expression like these will be evaluate in Project Executor
    bool withYield_{false};
    // outputs
    std::vector<std::string> geColNames_;
    // new yield to inject reserved properties for compatible with 1.0
    YieldClause* newYield_{nullptr};
    // input
    std::string inputVar_;
};

}   // namespace graph
}   // namespace nebula

#endif   // _VALIDATOR_FETCH_EDGES_VALIDATOR_H_
