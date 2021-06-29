/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/CombineFilterRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> CombineFilterRule::kInstance =
    std::unique_ptr<CombineFilterRule>(new CombineFilterRule());

CombineFilterRule::CombineFilterRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& CombineFilterRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter,
                                             {Pattern::create(graph::PlanNode::Kind::kFilter)});
    return pattern;
}

StatusOr<OptRule::TransformResult> CombineFilterRule::transform(
    OptContext* octx,
    const MatchedResult& matched) const {
    const auto* filterGroupNode = matched.node;
    const auto* filterAbove = filterGroupNode->node();
    DCHECK_EQ(filterAbove->kind(), PlanNode::Kind::kFilter);
    const auto* conditionAbove = static_cast<const graph::Filter*>(filterAbove)->condition();
    const auto& deps = matched.dependencies;
    const auto* filterGroup = filterGroupNode->group();
    auto* qctx = octx->qctx();
    auto* pool = qctx->objPool();

    TransformResult result;
    result.eraseAll = true;
    for (auto& dep : deps) {
        const auto* groupNode = dep.node;
        const auto* filterBelow = groupNode->node();
        DCHECK_EQ(filterBelow->kind(), PlanNode::Kind::kFilter);
        auto* newFilter = static_cast<graph::Filter*>(filterBelow->clone());
        const auto* conditionBelow = newFilter->condition();
        auto* conditionCombine =
            LogicalExpression::makeAnd(pool, conditionAbove->clone(), conditionBelow->clone());
        newFilter->setCondition(conditionCombine);
        newFilter->setOutputVar(filterAbove->outputVar());
        auto* newGroupNode = OptGroupNode::create(octx, newFilter, filterGroup);
        newGroupNode->setDeps(groupNode->dependencies());
        result.newGroupNodes.emplace_back(newGroupNode);
    }

    return result;
}

bool CombineFilterRule::match(OptContext* octx, const MatchedResult& matched) const {
    return OptRule::match(octx, matched);
}

std::string CombineFilterRule::toString() const {
    return "CombineFilterRule";
}

}   // namespace opt
}   // namespace nebula
