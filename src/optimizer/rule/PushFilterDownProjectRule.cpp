/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownProjectRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownProjectRule::kInstance =
    std::unique_ptr<PushFilterDownProjectRule>(new PushFilterDownProjectRule());

PushFilterDownProjectRule::PushFilterDownProjectRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownProjectRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter,
                                             {Pattern::create(graph::PlanNode::Kind::kProject)});
    return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownProjectRule::transform(
    OptContext* octx,
    const MatchedResult& matched) const {
    const auto* filterGroupNode = matched.node;
    const auto* filterNode = filterGroupNode->node();
    DCHECK_EQ(filterNode->kind(), PlanNode::Kind::kFilter);
    auto* oldFilterNode = static_cast<const graph::Filter*>(filterNode);
    const auto* projGroupNode = matched.dependencies.front().node;
    const auto* projNode = projGroupNode->node();
    DCHECK_EQ(projNode->kind(), PlanNode::Kind::kProject);
    const auto* oldProjNode = static_cast<const graph::Project*>(projNode);
    const auto* condition = oldFilterNode->condition();

    auto projColNames = oldProjNode->colNames();
    auto projColumns = oldProjNode->columns()->columns();
    std::unordered_map<std::string, Expression*> rewriteMap;
    // split the `condition` based on whether the propExprs comes from the left child
    auto picker = [&projColumns, &projColNames, &rewriteMap](const Expression* e) -> bool {
        auto varProps = graph::ExpressionUtils::collectAll(e,
                                                           {Expression::Kind::kTagProperty,
                                                            Expression::Kind::kEdgeProperty,
                                                            Expression::Kind::kInputProperty,
                                                            Expression::Kind::kVarProperty,
                                                            Expression::Kind::kDstProperty,
                                                            Expression::Kind::kSrcProperty});
        if (varProps.empty()) {
            return false;
        }
        std::vector<std::string> propNames;
        for (auto* expr : varProps) {
            DCHECK(graph::ExpressionUtils::isPropertyExpr(expr));
            propNames.emplace_back(static_cast<const PropertyExpression*>(expr)->prop());
        }
        for (size_t i = 0; i < projColNames.size(); ++i) {
            auto column = projColumns[i];
            auto colName = projColNames[i];
            auto iter =
                std::find_if(propNames.begin(), propNames.end(), [&colName](const auto& name) {
                    return !colName.compare(name);
                });
            if (iter == propNames.end()) continue;
            if (graph::ExpressionUtils::isPropertyExpr(column->expr())) {
                if (!column->alias().empty()) {
                    rewriteMap[colName] = column->expr();
                }
                continue;
            } else {
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
    // Rewrite PropertyExpr in filter's condition
    auto matcher = [&rewriteMap](const Expression* e) -> bool {
        if (!graph::ExpressionUtils::isPropertyExpr(e)) {
            return false;
        }
        auto& propName = static_cast<const PropertyExpression*>(e)->prop();
        return rewriteMap[propName];
    };
    auto rewriter = [&rewriteMap](const Expression* e) -> Expression* {
        DCHECK(graph::ExpressionUtils::isPropertyExpr(e));
        auto& propName = static_cast<const PropertyExpression*>(e)->prop();
        return rewriteMap[propName]->clone();
    };
    auto* newFilterPicked = rewriteMap.empty()
                                ? filterPicked
                                : graph::RewriteVisitor::transform(
                                      filterPicked, std::move(matcher), std::move(rewriter));

    // produce new Filter node below
    auto* newBelowFilterNode = graph::Filter::make(
        octx->qctx(), const_cast<graph::PlanNode*>(oldProjNode->dep()), newFilterPicked);
    newBelowFilterNode->setInputVar(oldProjNode->inputVar());
    auto newBelowFilterGroup = OptGroup::create(octx);
    auto newFilterGroupNode = newBelowFilterGroup->makeGroupNode(newBelowFilterNode);
    for (auto dep : projGroupNode->dependencies()) {
        newFilterGroupNode->dependsOn(dep);
    }

    // produce new Proj node
    auto* newProjNode = static_cast<graph::Project*>(oldProjNode->clone());
    newProjNode->setInputVar(newBelowFilterNode->outputVar());

    TransformResult result;
    result.eraseAll = true;
    if (filterUnpicked) {
        // produce new Filter node above
        auto* newAboveFilterNode = graph::Filter::make(octx->qctx(), newProjNode, filterUnpicked);
        newAboveFilterNode->setOutputVar(oldFilterNode->outputVar());
        auto newAboveFilterGroupNode =
            OptGroupNode::create(octx, newAboveFilterNode, filterGroupNode->group());

        auto newProjGroup = OptGroup::create(octx);
        auto newProjGroupNode = newProjGroup->makeGroupNode(newProjNode);
        newProjGroupNode->setDeps({newBelowFilterGroup});
        newAboveFilterGroupNode->setDeps({newProjGroup});
        result.newGroupNodes.emplace_back(newAboveFilterGroupNode);
    } else {
        newProjNode->setOutputVar(oldFilterNode->outputVar());
        auto newProjGroupNode = OptGroupNode::create(octx, newProjNode, filterGroupNode->group());
        newProjGroupNode->setDeps({newBelowFilterGroup});
        result.newGroupNodes.emplace_back(newProjGroupNode);
    }

    return result;
}

std::string PushFilterDownProjectRule::toString() const {
    return "PushFilterDownProjectRule";
}

}   // namespace opt
}   // namespace nebula
