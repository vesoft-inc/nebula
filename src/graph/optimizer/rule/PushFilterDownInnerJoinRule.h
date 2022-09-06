/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNINNERJOINRULE_H_
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNINNERJOINRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push down the filter items from the left subplan of [[InnerJoin]]
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Filter data early to optimize performance
//
//  Tranformation:
//  Before:
//
//  +-----------+-----------+
//  |         Filter        |
//  | ($left>3 and $right<4)|
//  +-----------+-----------+
//              |
//       +------+------+
//       |  InnerJoin   |
//       +------+------+
//
//  After:
//
//  +------+------+
//  |    Filter   |
//  |  ($right<4) |
//  +------+------+
//         |
//  +------+------+
//  |  InnerJoin   |
//  +------+------+
//         /
//  +------+------+
//  |    Filter   |
//  |  ($left>3)  |
//  +------+------+

class PushFilterDownInnerJoinRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *qctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownInnerJoinRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNINNERJOINRULE_H_
