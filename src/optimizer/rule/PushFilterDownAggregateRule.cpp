/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownAggregateRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "visitor/ExtractFilterExprVisitor.h"
#include "visitor/RewriteVisitor.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownAggregateRule::kInstance =
    std::unique_ptr<PushFilterDownAggregateRule>(new PushFilterDownAggregateRule());

PushFilterDownAggregateRule::PushFilterDownAggregateRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownAggregateRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter,
                                             {Pattern::create(graph::PlanNode::Kind::kAggregate)});
    return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownAggregateRule::transform(
    OptContext* octx,
    const MatchedResult& matched) const {
    auto* filterGroupNode = matched.node;
    auto* oldFilterNode = filterGroupNode->node();
    auto deps = matched.dependencies;
    DCHECK_EQ(deps.size(), 1);
    auto aggGroupNode = deps.front().node;
    auto* oldAggNode = aggGroupNode->node();
    DCHECK_EQ(oldFilterNode->kind(), PlanNode::Kind::kFilter);
    DCHECK_EQ(oldAggNode->kind(), PlanNode::Kind::kAggregate);
    auto* newFilterNode = static_cast<graph::Filter*>(oldFilterNode->clone());
    auto* newAggNode = static_cast<graph::Aggregate*>(oldAggNode->clone());
    const auto* condition = newFilterNode->condition();
    auto& groupItems = newAggNode->groupItems();

    // Check expression recursively to ensure no aggregate items in the filter
    auto varProps = graph::ExpressionUtils::collectAll(condition, {Expression::Kind::kVarProperty});
    if (varProps.empty()) {
        return TransformResult::noTransform();
    }
    std::vector<std::string> propNames;
    for (auto* expr : varProps) {
        DCHECK_EQ(expr->kind(), Expression::Kind::kVarProperty);
        propNames.emplace_back(static_cast<const VariablePropertyExpression*>(expr)->prop());
    }
    std::unordered_map<std::string, Expression*> rewriteMap;
    auto colNames = newAggNode->colNames();
    for (size_t i = 0; i < colNames.size(); ++i) {
        auto& colName = colNames[i];
        auto iter = std::find_if(propNames.begin(), propNames.end(), [&colName](const auto& name) {
            return !colName.compare(name);
        });
        if (iter == propNames.end()) continue;
        if (graph::ExpressionUtils::findAny(groupItems[i], {Expression::Kind::kAggregate})) {
            return TransformResult::noTransform();
        }
        rewriteMap[colName] = groupItems[i];
    }

    // Rewrite VariablePropertyExpr in filter's condition
    auto matcher = [&rewriteMap](const Expression* e) -> bool {
        if (e->kind() != Expression::Kind::kVarProperty) {
            return false;
        }
        auto& propName = static_cast<const VariablePropertyExpression*>(e)->prop();
        return rewriteMap.find(propName) != rewriteMap.end();
    };
    auto rewriter = [&rewriteMap](const Expression* e) -> Expression* {
        DCHECK_EQ(e->kind(), Expression::Kind::kVarProperty);
        auto& propName = static_cast<const VariablePropertyExpression*>(e)->prop();
        return rewriteMap[propName]->clone();
    };
    auto* newCondition =
        graph::RewriteVisitor::transform(condition, std::move(matcher), std::move(rewriter));
    newFilterNode->setCondition(newCondition);

    // Exchange planNode
    newAggNode->setOutputVar(oldFilterNode->outputVar());
    newFilterNode->setInputVar(oldAggNode->inputVar());
    DCHECK_EQ(oldAggNode->outputVar(), oldFilterNode->inputVar());
    newAggNode->setInputVar(oldAggNode->outputVar());
    newFilterNode->setOutputVar(oldAggNode->outputVar());

    // Push down filter's optGroup and embed newAggGroupNode into old filter's Group
    auto newAggGroupNode = OptGroupNode::create(octx, newAggNode, filterGroupNode->group());
    auto newFilterGroup = OptGroup::create(octx);
    auto newFilterGroupNode = newFilterGroup->makeGroupNode(newFilterNode);
    newAggGroupNode->dependsOn(newFilterGroup);
    for (auto dep : aggGroupNode->dependencies()) {
        newFilterGroupNode->dependsOn(dep);
    }

    TransformResult result;
    result.eraseAll = true;
    result.newGroupNodes.emplace_back(newAggGroupNode);
    return result;
}

std::string PushFilterDownAggregateRule::toString() const {
    return "PushFilterDownAggregateRule";
}

}   // namespace opt
}   // namespace nebula
