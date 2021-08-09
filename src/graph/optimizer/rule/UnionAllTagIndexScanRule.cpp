/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/UnionAllTagIndexScanRule.h"

using Kind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> UnionAllTagIndexScanRule::kInstance =
    std::unique_ptr<UnionAllTagIndexScanRule>(new UnionAllTagIndexScanRule());

UnionAllTagIndexScanRule::UnionAllTagIndexScanRule() {
    RuleSet::DefaultRules().addRule(this);
}

const Pattern& UnionAllTagIndexScanRule::pattern() const {
    static Pattern pattern =
        Pattern::create(Kind::kFilter, {Pattern::create(Kind::kTagIndexFullScan)});
    return pattern;
}

std::string UnionAllTagIndexScanRule::toString() const {
    return "UnionAllTagIndexScanRule";
}

}   // namespace opt
}   // namespace nebula
