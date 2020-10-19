/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/LimitPushDownRule.h"

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

using nebula::graph::GetNeighbors;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> LimitPushDownRule::kInstance =
    std::unique_ptr<LimitPushDownRule>(new LimitPushDownRule());

LimitPushDownRule::LimitPushDownRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern &LimitPushDownRule::pattern() const {
    static Pattern pattern =
        Pattern::create(graph::PlanNode::Kind::kLimit,
                        {Pattern::create(graph::PlanNode::Kind::kProject,
                                         {Pattern::create(graph::PlanNode::Kind::kGetNeighbors)})});
    return pattern;
}

StatusOr<OptRule::TransformResult> LimitPushDownRule::transform(
    QueryContext *qctx,
    const MatchedResult &matched) const {
    auto limitExpr = matched.node;
    auto projExpr = matched.dependencies.front().node;
    auto gnExpr = matched.dependencies.front().dependencies.front().node;

    const auto limit = static_cast<const Limit *>(limitExpr->node());
    const auto proj = static_cast<const Project *>(projExpr->node());
    const auto gn = static_cast<const GetNeighbors *>(gnExpr->node());

    int64_t limitRows = limit->offset() + limit->count();
    if (limitRows >= gn->limit()) {
        return TransformResult::noTransform();
    }

    auto newLimit = limit->clone(qctx);
    auto newLimitExpr = OptGroupExpr::create(qctx, newLimit, limitExpr->group());

    auto newProj = proj->clone(qctx);
    auto newProjGroup = OptGroup::create(qctx);
    auto newProjExpr = newProjGroup->makeGroupExpr(qctx, newProj);

    auto newGn = gn->clone(qctx);
    newGn->setLimit(limitRows);
    auto newGnGroup = OptGroup::create(qctx);
    auto newGnExpr = newGnGroup->makeGroupExpr(qctx, newGn);

    newLimitExpr->dependsOn(newProjGroup);
    newProjExpr->dependsOn(newGnGroup);
    for (auto dep : gnExpr->dependencies()) {
        newGnExpr->dependsOn(dep);
    }

    TransformResult result;
    result.eraseAll = true;
    result.eraseCurr = true;
    result.newGroupExprs.emplace_back(newLimitExpr);
    return result;
}

std::string LimitPushDownRule::toString() const {
    return "LimitPushDownRule";
}

}   // namespace opt
}   // namespace nebula
