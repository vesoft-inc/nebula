/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_TAGINDEXFULLSCANRULE_H_
#define GRAPH_OPTIMIZER_RULE_TAGINDEXFULLSCANRULE_H_

#include "graph/optimizer/rule/IndexFullScanBaseRule.h"

namespace nebula {
namespace opt {

// Apply the transformation of base class(IndexFullScanBaseRule::transform)
class TagIndexFullScanRule final : public IndexFullScanBaseRule {
 public:
  const Pattern& pattern() const override;
  std::string toString() const override;

 private:
  TagIndexFullScanRule();
  graph::IndexScan* scan(OptContext* ctx, const graph::PlanNode* node) const override;

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_TAGINDEXFULLSCANRULE_H_
