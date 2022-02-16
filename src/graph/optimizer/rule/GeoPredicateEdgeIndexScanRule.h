/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_GEOPREDICATEEDGEINDEXSCANRULE_H
#define GRAPH_OPTIMIZER_RULE_GEOPREDICATEEDGEINDEXSCANRULE_H

#include <memory>  // for uniq...
#include <string>  // for string

#include "graph/optimizer/rule/GeoPredicateIndexScanBaseRule.h"  // for GeoP...

namespace nebula {
namespace opt {
class OptRule;
class Pattern;

class OptRule;
class Pattern;

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
