/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownGetNbrsRule.h"

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

using nebula::Expression;
using nebula::graph::Filter;
using nebula::graph::GetNeighbors;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownGetNbrsRule::kInstance =
    std::unique_ptr<PushFilterDownGetNbrsRule>(new PushFilterDownGetNbrsRule());

PushFilterDownGetNbrsRule::PushFilterDownGetNbrsRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern &PushFilterDownGetNbrsRule::pattern() const {
    static Pattern pattern =
        Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kGetNeighbors)});
    return pattern;
}

bool PushFilterDownGetNbrsRule::match(OptContext *ctx, const MatchedResult &matched) const {
    if (!OptRule::match(ctx, matched)) {
        return false;
    }
    auto gn = static_cast<const GetNeighbors *>(matched.planNode({0, 0}));
    auto edgeProps = gn->edgeProps();
    // if fetching props of edge in GetNeighbors, let it go and do more checks in transform.
    // otherwise skip this rule.
    return edgeProps != nullptr && !edgeProps->empty();
}

StatusOr<OptRule::TransformResult> PushFilterDownGetNbrsRule::transform(
    OptContext *ctx,
    const MatchedResult &matched) const {
    auto filterGroupNode = matched.node;
    auto gnGroupNode = matched.dependencies.front().node;
    auto filter = static_cast<const Filter *>(filterGroupNode->node());
    auto gn = static_cast<const GetNeighbors *>(gnGroupNode->node());
    auto qctx = ctx->qctx();
    auto pool = qctx->objPool();
    auto condition = filter->condition()->clone();

    graph::ExtractFilterExprVisitor visitor(pool);
    condition->accept(&visitor);
    if (!visitor.ok()) {
        return TransformResult::noTransform();
    }

    auto remainedExpr = std::move(visitor).remainedExpr();
    OptGroupNode *newFilterGroupNode = nullptr;
    if (remainedExpr != nullptr) {
        auto newFilter = Filter::make(qctx, nullptr, remainedExpr);
        newFilter->setOutputVar(filter->outputVar());
        newFilter->setInputVar(filter->inputVar());
        newFilterGroupNode = OptGroupNode::create(ctx, newFilter, filterGroupNode->group());
    }

    auto newGNFilter = condition->encode();
    if (!gn->filter().empty()) {
        auto filterExpr = Expression::decode(pool, gn->filter());
        auto logicExpr = LogicalExpression::makeAnd(pool, condition, filterExpr);
        newGNFilter = logicExpr->encode();
    }

    auto newGN = static_cast<GetNeighbors *>(gn->clone());
    newGN->setFilter(newGNFilter);

    OptGroupNode *newGnGroupNode = nullptr;
    if (newFilterGroupNode != nullptr) {
        // Filter(A&&B)<-GetNeighbors(C) => Filter(A)<-GetNeighbors(B&&C)
        auto newGroup = OptGroup::create(ctx);
        newGnGroupNode = newGroup->makeGroupNode(newGN);
        newFilterGroupNode->dependsOn(newGroup);
    } else {
        // Filter(A)<-GetNeighbors(C) => GetNeighbors(A&&C)
        newGnGroupNode = OptGroupNode::create(ctx, newGN, filterGroupNode->group());
        newGN->setOutputVar(filter->outputVar());
    }

    for (auto dep : gnGroupNode->dependencies()) {
        newGnGroupNode->dependsOn(dep);
    }

    TransformResult result;
    result.eraseCurr = true;
    result.newGroupNodes.emplace_back(newFilterGroupNode ? newFilterGroupNode : newGnGroupNode);
    return result;
}

std::string PushFilterDownGetNbrsRule::toString() const {
    return "PushFilterDownGetNbrsRule";
}

}   // namespace opt
}   // namespace nebula
