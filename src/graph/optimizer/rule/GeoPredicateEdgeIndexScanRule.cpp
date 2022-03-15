/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/GeoPredicateEdgeIndexScanRule.h"

using Kind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> GeoPredicateEdgeIndexScanRule::kInstance =
    std::unique_ptr<GeoPredicateEdgeIndexScanRule>(new GeoPredicateEdgeIndexScanRule());

GeoPredicateEdgeIndexScanRule::GeoPredicateEdgeIndexScanRule() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& GeoPredicateEdgeIndexScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create(Kind::kFilter, {Pattern::create(Kind::kEdgeIndexFullScan)});
  return pattern;
}

std::string GeoPredicateEdgeIndexScanRule::toString() const {
  return "GeoPredicateEdgeIndexScanRule";
}

}  // namespace opt
}  // namespace nebula
