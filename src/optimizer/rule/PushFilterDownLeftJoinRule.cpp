/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownLeftJoinRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownLeftJoinRule::kInstance =
    std::unique_ptr<PushFilterDownLeftJoinRule>(new PushFilterDownLeftJoinRule());

PushFilterDownLeftJoinRule::PushFilterDownLeftJoinRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownLeftJoinRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter,
                                             {Pattern::create(graph::PlanNode::Kind::kLeftJoin)});
    return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownLeftJoinRule::transform(
    OptContext* octx,
    const MatchedResult& matched) const {
    auto* filterGroupNode = matched.node;
    auto* oldFilterNode = filterGroupNode->node();
    auto deps = matched.dependencies;
    DCHECK_EQ(deps.size(), 1);
    auto leftJoinGroupNode = deps.front().node;
    auto* leftJoinNode = leftJoinGroupNode->node();
    DCHECK_EQ(oldFilterNode->kind(), PlanNode::Kind::kFilter);
    DCHECK_EQ(leftJoinNode->kind(), PlanNode::Kind::kLeftJoin);
    auto* oldLeftJoinNode = static_cast<graph::LeftJoin*>(leftJoinNode);
    const auto* condition = static_cast<graph::Filter*>(oldFilterNode)->condition();
    DCHECK(condition);
    const std::pair<std::string, int64_t>& leftVar = oldLeftJoinNode->leftVar();
    auto symTable = octx->qctx()->symTable();
    std::vector<std::string> leftVarColNames = symTable->getVar(leftVar.first)->colNames;

    // split the `condition` based on whether the varPropExpr comes from the left child
    auto picker = [&leftVarColNames](const Expression* e) -> bool {
        auto varProps = graph::ExpressionUtils::collectAll(e, {Expression::Kind::kVarProperty});
        if (varProps.empty()) {
            return false;
        }
        std::vector<std::string> propNames;
        for (auto* expr : varProps) {
            DCHECK(expr->kind() == Expression::Kind::kVarProperty);
            propNames.emplace_back(static_cast<const VariablePropertyExpression*>(expr)->prop());
        }
        for (auto prop : propNames) {
            auto iter = std::find_if(leftVarColNames.begin(),
                                     leftVarColNames.end(),
                                     [&prop](std::string item) { return !item.compare(prop); });
            if (iter == leftVarColNames.end()) {
                return false;
            }
        }
        return true;
    };
    Expression* filterPicked = nullptr;
    Expression* filterUnpicked = nullptr;
    graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, &filterUnpicked);

    if (!filterPicked) {
        return TransformResult::noTransform();
    }

    // produce new left Filter node
    auto* newLeftFilterNode = graph::Filter::make(
        octx->qctx(),
        const_cast<graph::PlanNode*>(oldLeftJoinNode->dep()),
        graph::ExpressionUtils::rewriteInnerVar(filterPicked, leftVar.first));
    newLeftFilterNode->setInputVar(leftVar.first);
    newLeftFilterNode->setColNames(leftVarColNames);
    auto newFilterGroup = OptGroup::create(octx);
    auto newFilterGroupNode = newFilterGroup->makeGroupNode(newLeftFilterNode);
    for (auto dep : leftJoinGroupNode->dependencies()) {
        newFilterGroupNode->dependsOn(dep);
    }
    auto newLeftFilterOutputVar = newLeftFilterNode->outputVar();

    // produce new LeftJoin node
    auto* newLeftJoinNode = static_cast<graph::LeftJoin*>(oldLeftJoinNode->clone());
    newLeftJoinNode->setLeftVar({newLeftFilterOutputVar, 0});
    const std::vector<Expression*>& hashKeys = oldLeftJoinNode->hashKeys();
    std::vector<Expression*> newHashKeys;
    for (auto* k : hashKeys) {
        newHashKeys.emplace_back(
            graph::ExpressionUtils::rewriteInnerVar(k, newLeftFilterOutputVar));
    }
    newLeftJoinNode->setHashKeys(newHashKeys);

    TransformResult result;
    result.eraseAll = true;
    if (filterUnpicked) {
        auto* newAboveFilterNode = graph::Filter::make(octx->qctx(), newLeftJoinNode);
        newAboveFilterNode->setOutputVar(oldFilterNode->outputVar());
        newAboveFilterNode->setCondition(filterUnpicked);
        auto newAboveFilterGroupNode =
            OptGroupNode::create(octx, newAboveFilterNode, filterGroupNode->group());

        auto newLeftJoinGroup = OptGroup::create(octx);
        auto newLeftJoinGroupNode = newLeftJoinGroup->makeGroupNode(newLeftJoinNode);
        newAboveFilterGroupNode->setDeps({newLeftJoinGroup});
        newLeftJoinGroupNode->setDeps({newFilterGroup});
        result.newGroupNodes.emplace_back(newAboveFilterGroupNode);
    } else {
        newLeftJoinNode->setOutputVar(oldFilterNode->outputVar());
        newLeftJoinNode->setColNames(oldLeftJoinNode->colNames());
        auto newLeftJoinGroupNode =
            OptGroupNode::create(octx, newLeftJoinNode, filterGroupNode->group());
        newLeftJoinGroupNode->setDeps({newFilterGroup});
        result.newGroupNodes.emplace_back(newLeftJoinGroupNode);
    }
    return result;
}

std::string PushFilterDownLeftJoinRule::toString() const {
    return "PushFilterDownLeftJoinRule";
}

}   // namespace opt
}   // namespace nebula
