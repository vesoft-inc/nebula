/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHTOPNDOWNINDEXSCANRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHTOPNDOWNINDEXSCANRULE_H

#include <initializer_list>

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embedding TopN factors into Storage layer
//  Required conditions:
//   1. Match the pattern
//   2. YieldColumn Expression is kTagProperty or kEdgeProperty
//  Benefits:
//   1. Limit data early to optimize performance
//
//  Transformation:
//  Before:
//
//  +----------+----------+
//  |        TopN         |
//  +----------+----------+
//             |
//  +----------+----------+
//  |       Project       |
//  +----------+----------+
//             |
//   +---------+---------+
//   |  kIndexScanKinds  |
//   +---------+---------+
//
//  After:
//
//  +----------+----------+
//  |        TopN         |
//  +----------+----------+
//             |
//  +----------+----------+
//  |       Project       |
//  +----------+----------+
//             |
//   +---------+---------+
//   |  kIndexScanKinds  |
//   | (limit_=limitRows)|
//   |(orderBy_=orderBys)|
//   +---------+---------+

class PushTopNDownIndexScanRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushTopNDownIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
  static const std::initializer_list<graph::PlanNode::Kind> kIndexScanKinds;
};

}  // namespace opt
}  // namespace nebula
#endif
