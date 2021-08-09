/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/UnionAllEdgeIndexScanRule.h"

using Kind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> UnionAllEdgeIndexScanRule::kInstance =
    std::unique_ptr<UnionAllEdgeIndexScanRule>(new UnionAllEdgeIndexScanRule());

UnionAllEdgeIndexScanRule::UnionAllEdgeIndexScanRule() {
    RuleSet::DefaultRules().addRule(this);
}

const Pattern& UnionAllEdgeIndexScanRule::pattern() const {
    static Pattern pattern =
        Pattern::create(Kind::kFilter, {Pattern::create(Kind::kEdgeIndexFullScan)});
    return pattern;
}

std::string UnionAllEdgeIndexScanRule::toString() const {
    return "UnionAllEdgeIndexScanRule";
}

}   // namespace opt
}   // namespace nebula
