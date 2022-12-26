/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHSTEPSAMPLEDOWNGETNEIGHBORSRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHSTEPSAMPLEDOWNGETNEIGHBORSRULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embedding limit to [[GetNeighbors]]
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Limit data early to optimize performance
//  Query example:
//   GO 2 STEPS FROM "Tim Duncan" over like YIELD like._dst SAMPLE [2,3]
//  Transformation:
//  Before:
//
//  +----------+----------+
//  |        Sample       |
//  |(SubscriptExpression)|
//  +----------+----------+
//             |
//   +---------+---------+
//   |    GetNeighbors   |
//   +---------+---------+
//
//  After:
//
// +----------+----------+
// |        Sample       |
// |(SubscriptExpression)|
// +----------+----------+
//            |
// +----------+----------+
// |     GetNeighbors    |
// |(SubscriptExpression)|
// +----------+----------+

class PushStepSampleDownGetNeighborsRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushStepSampleDownGetNeighborsRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
