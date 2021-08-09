/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/MergeGetNbrsAndDedupRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

using nebula::graph::Dedup;
using nebula::graph::GetNeighbors;
using nebula::graph::PlanNode;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> MergeGetNbrsAndDedupRule::kInstance =
    std::unique_ptr<MergeGetNbrsAndDedupRule>(new MergeGetNbrsAndDedupRule());

MergeGetNbrsAndDedupRule::MergeGetNbrsAndDedupRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern &MergeGetNbrsAndDedupRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kGetNeighbors,
                                             {Pattern::create(graph::PlanNode::Kind::kDedup)});
    return pattern;
}

StatusOr<OptRule::TransformResult> MergeGetNbrsAndDedupRule::transform(
    OptContext *octx,
    const MatchedResult &matched) const {
    const OptGroupNode *optGN = matched.node;
    const OptGroupNode *optDedup = matched.dependencies.back().node;
    DCHECK_EQ(optGN->node()->kind(), PlanNode::Kind::kGetNeighbors);
    DCHECK_EQ(optDedup->node()->kind(), PlanNode::Kind::kDedup);
    auto gn = static_cast<const GetNeighbors *>(optGN->node());
    auto dedup = static_cast<const Dedup *>(optDedup->node());

    auto newGN = static_cast<GetNeighbors *>(gn->clone());
    if (!newGN->dedup()) {
        newGN->setDedup();
    }
    newGN->setInputVar(dedup->inputVar());
    auto newOptGV = OptGroupNode::create(octx, newGN, optGN->group());
    for (auto dep : optDedup->dependencies()) {
        newOptGV->dependsOn(dep);
    }
    TransformResult result;
    result.eraseCurr = true;
    result.newGroupNodes.emplace_back(newOptGV);
    return result;
}

std::string MergeGetNbrsAndDedupRule::toString() const {
    return "MergeGetNbrsAndDedupRule";
}

}   // namespace opt
}   // namespace nebula
