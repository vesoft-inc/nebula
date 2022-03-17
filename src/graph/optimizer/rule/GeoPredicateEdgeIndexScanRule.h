/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_GEOPREDICATEEDGEINDEXSCANRULE_H
#define GRAPH_OPTIMIZER_RULE_GEOPREDICATEEDGEINDEXSCANRULE_H

#include "graph/optimizer/rule/GeoPredicateIndexScanBaseRule.h"

namespace nebula {
namespace opt {

// Apply the transformation of base class(GeoPredicateIndexScanBaseRule::transform)
class GeoPredicateEdgeIndexScanRule final : public GeoPredicateIndexScanBaseRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  GeoPredicateEdgeIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
