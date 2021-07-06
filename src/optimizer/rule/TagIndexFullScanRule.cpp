/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/TagIndexFullScanRule.h"
#include "optimizer/OptContext.h"
#include "planner/plan/Scan.h"

using Kind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> TagIndexFullScanRule::kInstance =
    std::unique_ptr<TagIndexFullScanRule>(new TagIndexFullScanRule());

TagIndexFullScanRule::TagIndexFullScanRule() {
    RuleSet::DefaultRules().addRule(this);
}

const Pattern& TagIndexFullScanRule::pattern() const {
    static Pattern pattern = Pattern::create(Kind::kTagIndexFullScan);
    return pattern;
}

std::string TagIndexFullScanRule::toString() const {
    return "TagIndexFullScanRule";
}

graph::IndexScan* TagIndexFullScanRule::scan(OptContext* ctx, const graph::PlanNode* node) const {
    auto scan = static_cast<const graph::TagIndexFullScan*>(node);
    return graph::TagIndexFullScan::make(ctx->qctx(), nullptr, scan->tagName());
}

}   // namespace opt
}   // namespace nebula
