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
#include "optimizer/OptGroup.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "visitor/ExtractFilterExprVisitor.h"

using nebula::graph::Filter;
using nebula::graph::GetNeighbors;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownGetNbrsRule::kInstance =
    std::unique_ptr<PushFilterDownGetNbrsRule>(new PushFilterDownGetNbrsRule());

PushFilterDownGetNbrsRule::PushFilterDownGetNbrsRule() {
    RuleSet::queryRules().addRule(this);
}

bool PushFilterDownGetNbrsRule::match(const OptGroupExpr *groupExpr) const {
    auto pair = findMatchedGroupExpr(groupExpr);
    if (!pair.first) {
        return false;
    }

    return true;
}

Status PushFilterDownGetNbrsRule::transform(QueryContext *qctx,
                                            const OptGroupExpr *groupExpr,
                                            TransformResult *result) const {
    auto pair = findMatchedGroupExpr(groupExpr);
    auto filter = static_cast<const Filter *>(groupExpr->node());
    auto gn = static_cast<const GetNeighbors *>(pair.second->node());

    auto condition = filter->condition()->clone();
    graph::ExtractFilterExprVisitor visitor;
    condition->accept(&visitor);
    if (!visitor.ok()) {
        result->eraseCurr = false;
        result->eraseAll = false;
        return Status::OK();
    }

    auto pool = qctx->objPool();
    auto remainedExpr = std::move(visitor).remainedExpr();
    OptGroupExpr *newFilterGroupExpr = nullptr;
    if (remainedExpr != nullptr) {
        auto newFilter = Filter::make(qctx, nullptr, pool->add(remainedExpr.release()));
        newFilterGroupExpr = OptGroupExpr::create(qctx, newFilter, groupExpr->group());
    }

    auto newGNFilter = condition->encode();
    if (!gn->filter().empty()) {
        auto filterExpr = Expression::decode(gn->filter());
        LogicalExpression logicExpr(
            Expression::Kind::kLogicalAnd, condition.release(), filterExpr.release());
        newGNFilter = logicExpr.encode();
    }

    auto newGN = cloneGetNbrs(qctx, gn);
    newGN->setFilter(newGNFilter);

    OptGroupExpr *newGroupExpr = nullptr;
    if (newFilterGroupExpr != nullptr) {
        // Filter(A&&B)->GetNeighbors(C) => Filter(A)->GetNeighbors(B&&C)
        auto newGroup = OptGroup::create(qctx);
        newGroupExpr = OptGroupExpr::create(qctx, newGN, newGroup);
        newFilterGroupExpr->dependsOn(newGroup);
    } else {
        // Filter(A)->GetNeighbors(C) => GetNeighbors(A&&C)
        newGroupExpr = OptGroupExpr::create(qctx, newGN, groupExpr->group());
        newGN->setOutputVar(filter->varName());
    }

    for (auto dep : pair.second->dependencies()) {
        newGroupExpr->dependsOn(dep);
    }
    result->newGroupExprs.emplace_back(newFilterGroupExpr ? newFilterGroupExpr : newGroupExpr);
    result->eraseAll = true;
    result->eraseCurr = true;

    return Status::OK();
}

std::string PushFilterDownGetNbrsRule::toString() const {
    return "PushFilterDownGetNbrsRule";
}

std::pair<bool, const OptGroupExpr *> PushFilterDownGetNbrsRule::findMatchedGroupExpr(
    const OptGroupExpr *groupExpr) const {
    auto node = groupExpr->node();
    if (node->kind() != PlanNode::Kind::kFilter) {
        return std::make_pair(false, nullptr);
    }

    for (auto dep : groupExpr->dependencies()) {
        for (auto expr : dep->groupExprs()) {
            if (expr->node()->kind() == PlanNode::Kind::kGetNeighbors) {
                return std::make_pair(true, expr);
            }
        }
    }
    return std::make_pair(false, nullptr);
}

GetNeighbors *PushFilterDownGetNbrsRule::cloneGetNbrs(QueryContext *qctx,
                                                      const GetNeighbors *gn) const {
    auto newGN = GetNeighbors::make(qctx, nullptr, gn->space());
    newGN->setSrc(gn->src());
    newGN->setEdgeTypes(gn->edgeTypes());
    newGN->setEdgeDirection(gn->edgeDirection());
    newGN->setDedup(gn->dedup());
    newGN->setRandom(gn->random());
    newGN->setLimit(gn->limit());
    newGN->setInputVar(gn->inputVar());

    if (gn->vertexProps()) {
        auto vertexProps = *gn->vertexProps();
        auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(std::move(vertexProps));
        newGN->setVertexProps(std::move(vertexPropsPtr));
    }

    if (gn->edgeProps()) {
        auto edgeProps = *gn->edgeProps();
        auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
        newGN->setEdgeProps(std::move(edgePropsPtr));
    }

    if (gn->statProps()) {
        auto statProps = *gn->statProps();
        auto statPropsPtr = std::make_unique<decltype(statProps)>(std::move(statProps));
        newGN->setStatProps(std::move(statPropsPtr));
    }

    if (gn->exprs()) {
        auto exprs = *gn->exprs();
        auto exprsPtr = std::make_unique<decltype(exprs)>(std::move(exprs));
        newGN->setExprs(std::move(exprsPtr));
    }
    return newGN;
}

}   // namespace opt
}   // namespace nebula
