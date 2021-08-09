/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/MergeGetVerticesAndDedupRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

using nebula::graph::Dedup;
using nebula::graph::GetVertices;
using nebula::graph::PlanNode;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> MergeGetVerticesAndDedupRule::kInstance =
    std::unique_ptr<MergeGetVerticesAndDedupRule>(new MergeGetVerticesAndDedupRule());

MergeGetVerticesAndDedupRule::MergeGetVerticesAndDedupRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern &MergeGetVerticesAndDedupRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kGetVertices,
                                             {Pattern::create(graph::PlanNode::Kind::kDedup)});
    return pattern;
}

StatusOr<OptRule::TransformResult> MergeGetVerticesAndDedupRule::transform(
    OptContext *ctx,
    const MatchedResult &matched) const {
    const OptGroupNode *optGV = matched.node;
    const OptGroupNode *optDedup = matched.dependencies.back().node;
    DCHECK_EQ(optGV->node()->kind(), PlanNode::Kind::kGetVertices);
    DCHECK_EQ(optDedup->node()->kind(), PlanNode::Kind::kDedup);
    auto gv = static_cast<const GetVertices *>(optGV->node());
    auto dedup = static_cast<const Dedup *>(optDedup->node());
    auto newGV = static_cast<GetVertices *>(gv->clone());
    if (!newGV->dedup()) {
        newGV->setDedup();
    }
    newGV->setInputVar(dedup->inputVar());
    auto newOptGV = OptGroupNode::create(ctx, newGV, optGV->group());
    for (auto dep : optDedup->dependencies()) {
        newOptGV->dependsOn(dep);
    }
    TransformResult result;
    result.eraseCurr = true;
    result.newGroupNodes.emplace_back(newOptGV);
    return result;
}

std::string MergeGetVerticesAndDedupRule::toString() const {
    return "MergeGetVerticesAndDedupRule";
}

}   // namespace opt
}   // namespace nebula
