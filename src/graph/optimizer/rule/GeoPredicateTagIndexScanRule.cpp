/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/GeoPredicateTagIndexScanRule.h"

using Kind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> GeoPredicateTagIndexScanRule::kInstance =
    std::unique_ptr<GeoPredicateTagIndexScanRule>(new GeoPredicateTagIndexScanRule());

GeoPredicateTagIndexScanRule::GeoPredicateTagIndexScanRule() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& GeoPredicateTagIndexScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create(Kind::kFilter, {Pattern::create(Kind::kTagIndexFullScan)});
  return pattern;
}

std::string GeoPredicateTagIndexScanRule::toString() const {
  return "GeoPredicateTagIndexScanRule";
}

}  // namespace opt
}  // namespace nebula
