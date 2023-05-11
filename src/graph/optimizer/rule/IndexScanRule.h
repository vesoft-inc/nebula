/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef GRAPH_OPTIMIZER_INDEXSCANRULE_H_
#define GRAPH_OPTIMIZER_INDEXSCANRULE_H_

#include "graph/optimizer/OptRule.h"
#include "graph/optimizer/OptimizerUtils.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace opt {

class OptContext;

// Select the optimal index according to the preset filter inside [[IndexScan]]
class IndexScanRule final : public OptRule {
  FRIEND_TEST(IndexScanRuleTest, BoundValueTest);
  FRIEND_TEST(IndexScanRuleTest, IQCtxTest);
  FRIEND_TEST(IndexScanRuleTest, BoundValueRangeTest);

 public:
  const Pattern& pattern() const override;

  bool match(OptContext* ctx, const MatchedResult& matched) const override;
  StatusOr<TransformResult> transform(OptContext* ctx, const MatchedResult& matched) const override;
  std::string toString() const override;

 private:
  static std::unique_ptr<OptRule> kInstance;

  IndexScanRule();

  Expression* filterExpr(const OptGroupNode* groupNode) const;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_INDEXSCANRULE_H_
