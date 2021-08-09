/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/MergeGetVerticesAndProjectRule.h"

#include "common/expression/PropertyExpression.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

using nebula::Expression;
using nebula::InputPropertyExpression;
using nebula::graph::GetVertices;
using nebula::graph::PlanNode;
using nebula::graph::Project;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> MergeGetVerticesAndProjectRule::kInstance =
    std::unique_ptr<MergeGetVerticesAndProjectRule>(new MergeGetVerticesAndProjectRule());

MergeGetVerticesAndProjectRule::MergeGetVerticesAndProjectRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern &MergeGetVerticesAndProjectRule::pattern() const {
    static Pattern pattern =
        Pattern::create(PlanNode::Kind::kGetVertices, {Pattern::create(PlanNode::Kind::kProject)});
    return pattern;
}

bool MergeGetVerticesAndProjectRule::match(OptContext *ctx, const MatchedResult &matched) const {
    if (!OptRule::match(ctx, matched)) {
        return false;
    }
    const auto *optGV = matched.node;
    auto gv = static_cast<const GetVertices *>(optGV->node());
    const auto *optProj = matched.dependencies.back().node;
    auto proj = static_cast<const Project *>(optProj->node());
    // The projection must project only one column which expression will be assigned to
    // the src expression of GetVertices
    auto srcExpr = gv->src();
    const auto &columns = proj->colNames();
    if (srcExpr->kind() != Expression::Kind::kInputProperty || columns.size() != 1UL) {
        return false;
    }
    auto inputPropExpr = static_cast<const InputPropertyExpression *>(srcExpr);
    return columns.back() == inputPropExpr->prop();
}

StatusOr<OptRule::TransformResult> MergeGetVerticesAndProjectRule::transform(
    OptContext *ctx,
    const MatchedResult &matched) const {
    const OptGroupNode *optGV = matched.node;
    const OptGroupNode *optProj = matched.dependencies.back().node;
    DCHECK_EQ(optGV->node()->kind(), PlanNode::Kind::kGetVertices);
    DCHECK_EQ(optProj->node()->kind(), PlanNode::Kind::kProject);
    auto gv = static_cast<const GetVertices *>(optGV->node());
    auto project = static_cast<const Project *>(optProj->node());
    auto newGV = static_cast<GetVertices *>(gv->clone());
    auto column = project->columns()->back();
    auto srcExpr = column->expr()->clone();
    newGV->setSrc(srcExpr);
    newGV->setInputVar(project->inputVar());
    auto newOptGV = OptGroupNode::create(ctx, newGV, optGV->group());
    for (auto dep : optProj->dependencies()) {
        newOptGV->dependsOn(dep);
    }
    TransformResult result;
    result.eraseCurr = true;
    result.newGroupNodes.emplace_back(newOptGV);
    return result;
}

std::string MergeGetVerticesAndProjectRule::toString() const {
    return "MergeGetVerticesAndProjectRule";
}

}   // namespace opt
}   // namespace nebula
