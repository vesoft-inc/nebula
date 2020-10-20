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
    auto limitGroupNode = matched.node;
    auto projGroupNode = matched.dependencies.front().node;
    auto gnGroupNode = matched.dependencies.front().dependencies.front().node;

    const auto limit = static_cast<const Limit *>(limitGroupNode->node());
    const auto proj = static_cast<const Project *>(projGroupNode->node());
    const auto gn = static_cast<const GetNeighbors *>(gnGroupNode->node());

    int64_t limitRows = limit->offset() + limit->count();
    if (limitRows >= gn->limit()) {
        return TransformResult::noTransform();
    }

    auto newLimit = limit->clone(qctx);
    auto newLimitGroupNode = OptGroupNode::create(qctx, newLimit, limitGroupNode->group());

    auto newProj = proj->clone(qctx);
    auto newProjGroup = OptGroup::create(qctx);
    auto newProjGroupNode = newProjGroup->makeGroupNode(qctx, newProj);

    auto newGn = gn->clone(qctx);
    newGn->setLimit(limitRows);
    auto newGnGroup = OptGroup::create(qctx);
    auto newGnGroupNode = newGnGroup->makeGroupNode(qctx, newGn);

    newLimitGroupNode->dependsOn(newProjGroup);
    newProjGroupNode->dependsOn(newGnGroup);
    for (auto dep : gnGroupNode->dependencies()) {
        newGnGroupNode->dependsOn(dep);
    }

    TransformResult result;
    result.eraseAll = true;
    result.newGroupNodes.emplace_back(newLimitGroupNode);
    return result;
}

std::string LimitPushDownRule::toString() const {
    return "LimitPushDownRule";
}

}   // namespace opt
}   // namespace nebula
