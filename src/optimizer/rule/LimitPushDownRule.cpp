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
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
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
    OptContext *octx,
    const MatchedResult &matched) const {
    auto limitGroupNode = matched.node;
    auto projGroupNode = matched.dependencies.front().node;
    auto gnGroupNode = matched.dependencies.front().dependencies.front().node;

    const auto limit = static_cast<const Limit *>(limitGroupNode->node());
    const auto proj = static_cast<const Project *>(projGroupNode->node());
    const auto gn = static_cast<const GetNeighbors *>(gnGroupNode->node());

    int64_t limitRows = limit->offset() + limit->count();
    if (gn->limit() >= 0 && limitRows >= gn->limit()) {
        return TransformResult::noTransform();
    }

    auto newLimit = static_cast<Limit *>(limit->clone());
    auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

    auto newProj = static_cast<Project *>(proj->clone());
    auto newProjGroup = OptGroup::create(octx);
    auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

    auto newGn = static_cast<GetNeighbors *>(gn->clone());
    newGn->setLimit(limitRows);
    auto newGnGroup = OptGroup::create(octx);
    auto newGnGroupNode = newGnGroup->makeGroupNode(newGn);

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
