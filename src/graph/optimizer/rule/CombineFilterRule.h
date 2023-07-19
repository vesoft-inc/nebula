/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_COMBINEFILTERRULE_H_
#define GRAPH_OPTIMIZER_RULE_COMBINEFILTERRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Combines two [[Filter]] nodes into one and connect the filter expressions with `LogicalAnd`
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. reduces the expression iterated times
//
//  Transformation:
//  Before:
//
//  +-----+-----+
//  | Filter(A) |
//  +-----+-----+
//        |
//  +-----+-----+
//  | Filter(B) |
//  +-----+-----+
//
//  After:
//
//  +--------+--------+
//  | Filter(A and B) |
//  +--------+--------+
//

class CombineFilterRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  CombineFilterRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_COMBINEFILTERRULE_H_
