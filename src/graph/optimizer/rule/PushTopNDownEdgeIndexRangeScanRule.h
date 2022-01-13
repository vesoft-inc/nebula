/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHTOPNDOWNEDGEINDEXRANGESCANRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHTOPNDOWNEDGEINDEXRANGESCANRULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

class PushTopNDownEdgeIndexRangeScanRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushTopNDownEdgeIndexRangeScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
