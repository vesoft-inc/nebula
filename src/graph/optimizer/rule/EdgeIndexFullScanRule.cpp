/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/EdgeIndexFullScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/planner/plan/Query.h"
#include "graph/planner/plan/Scan.h"

using nebula::graph::EdgeIndexFullScan;
using nebula::graph::IndexScan;

using Kind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> EdgeIndexFullScanRule::kInstance =
    std::unique_ptr<EdgeIndexFullScanRule>(new EdgeIndexFullScanRule());

EdgeIndexFullScanRule::EdgeIndexFullScanRule() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& EdgeIndexFullScanRule::pattern() const {
  static Pattern pattern = Pattern::create(Kind::kEdgeIndexFullScan);
  return pattern;
}

std::string EdgeIndexFullScanRule::toString() const {
  return "EdgeIndexFullScanRule";
}

IndexScan* EdgeIndexFullScanRule::scan(OptContext* ctx, const graph::PlanNode* node) const {
  auto scan = static_cast<const EdgeIndexFullScan*>(node);
  return EdgeIndexFullScan::make(ctx->qctx(), nullptr, scan->edgeType());
}

}  // namespace opt
}  // namespace nebula
