/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNLEFTJOINRULE_H_
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNLEFTJOINRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push down the filter items from the left subplan of [[LeftJoin]]
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
//       |  LeftJoin   |
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
//  |  LeftJoin   |
//  +------+------+
//         /
//  +------+------+
//  |    Filter   |
//  |  ($left>3)  |
//  +------+------+

class PushFilterDownLeftJoinRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *qctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownLeftJoinRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNLEFTJOINRULE_H_
