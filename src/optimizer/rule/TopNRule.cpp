/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/TopNRule.h"

#include "common/expression/BinaryExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/UnaryExpression.h"
#include "optimizer/OptGroup.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "visitor/ExtractFilterExprVisitor.h"

using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Sort;
using nebula::graph::TopN;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> TopNRule::kInstance = std::unique_ptr<TopNRule>(new TopNRule());

TopNRule::TopNRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern &TopNRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                             {Pattern::create(graph::PlanNode::Kind::kSort)});
    return pattern;
}

StatusOr<OptRule::TransformResult> TopNRule::transform(QueryContext *qctx,
                                                       const MatchedResult &matched) const {
    auto limitExpr = matched.node;
    auto sortExpr = matched.dependencies.front().node;
    auto limit = static_cast<const Limit *>(limitExpr->node());
    auto sort = static_cast<const Sort *>(sortExpr->node());

    // Currently, we cannot know the total amount of input data,
    // so only apply topn rule when offset of limit is 0
    if (limit->offset() != 0) {
        return TransformResult::noTransform();
    }

    auto topn = TopN::make(qctx, nullptr, sort->factors(), limit->offset(), limit->count());
    topn->setOutputVar(limit->outputVar());
    topn->setInputVar(sort->inputVar());
    topn->setColNames(sort->colNames());
    auto topnExpr = OptGroupExpr::create(qctx, topn, limitExpr->group());
    for (auto dep : sortExpr->dependencies()) {
        topnExpr->dependsOn(dep);
    }

    TransformResult result;
    result.newGroupExprs.emplace_back(topnExpr);
    result.eraseAll = true;
    result.eraseCurr = true;
    return result;
}

std::string TopNRule::toString() const {
    return "TopNRule";
}

}   // namespace opt
}   // namespace nebula
