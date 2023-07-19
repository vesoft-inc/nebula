/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_UNIONALLEDGEINDEXSCANRULE_H_
#define GRAPH_OPTIMIZER_RULE_UNIONALLEDGEINDEXSCANRULE_H_

#include "graph/optimizer/rule/UnionAllIndexScanBaseRule.h"

namespace nebula {
namespace opt {

// Apply the transformation of base class(UnionAllIndexScanBaseRule::transform)
class UnionAllEdgeIndexScanRule final : public UnionAllIndexScanBaseRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  UnionAllEdgeIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_UNIONALLEDGEINDEXSCANRULE_H_
