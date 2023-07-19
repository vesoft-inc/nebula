/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNPROJECTRULE_H_
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNPROJECTRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push down the filter items
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Filter data early to optimize performance
//
//  Transformation:
//  Before:
//
//  +-------------+-------------+
//  |           Filter          |
//  |($p1>3 and $p2<4 and $p1<9)|
//  +-------------+-------------+
//                |
//      +---------+---------+
//      |      Project      |
//      |(log($p) AS p1,$p2)|
//      +---------+---------+
//
//  After:
//
//  +--------+--------+
//  |      Filter     |
//  |($p1>3 and $p1<9)|
//  +--------+--------+
//           |
// +---------+---------+
// |      Project      |
// |(log($p) AS p1,$p2)|
// +---------+---------+
//           |
//    +------+------+
//    |   Filter    |
//    |   ($p2<4)   |
//    +------+------+

class PushFilterDownProjectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *qctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownProjectRule();

  static bool checkColumnExprKind(const Expression *expr);

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNPROJECTRULE_H_
