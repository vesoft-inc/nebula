/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_TOPNRULE_H_
#define GRAPH_OPTIMIZER_RULE_TOPNRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Convert [[Limit]] and [[Sort]] into [[TopN]]
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Equivalently transform the plan to implement a better topN algorithm at the computing layer
//
//  Transformation:
//  Before:
//
//  +--------+--------+
//  |      Limit      |
//  +--------+--------+
//           |
// +---------+---------+
// |       Sort        |
// +---------+---------+
//
//  After:
//
//
// +---------+---------+
// |        TopN       |
// +---------+---------+

class TopNRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  TopNRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_TOPNRULE_H_
