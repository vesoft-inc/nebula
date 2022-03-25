/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_GEOPREDICATETAGINDEXSCANRULE_H
#define GRAPH_OPTIMIZER_RULE_GEOPREDICATETAGINDEXSCANRULE_H

#include "graph/optimizer/rule/GeoPredicateIndexScanBaseRule.h"

namespace nebula {
namespace opt {

// Apply the transformation of base class(GeoPredicateIndexScanBaseRule::transform)
class GeoPredicateTagIndexScanRule final : public GeoPredicateIndexScanBaseRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  GeoPredicateTagIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
