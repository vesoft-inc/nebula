/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNGETNBRSRULE_H_
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNGETNBRSRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embed the [[Filter]] into [[GetNeighbors]]
//  Required conditions:
//   1. Match the pattern
//   2. Filter contains subexpressions that meet pushdown conditions
//  Benefits:
//   1. Filter data early to optimize performance
//
//  Tranformation:
//  Before:
//
//  +------------------+------------------+
//  |                Filter               |
//  |($^.player.age>3 and $$.player.age<4)|
//  +------------------+------------------+
//                     |
//              +------+------+
//              | GetNeighbors|
//              +------+------+
//
//  After:
//
//  +--------+--------+
//  |    Filter       |
//  |($$.player.age<4)|
//  +--------+--------+
//           |
//  +--------+--------+
//  |  GetNeighbors   |
//  |($^.player.age>3)|
//  +--------+--------+

class PushFilterDownGetNbrsRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownGetNbrsRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNGETNBRSRULE_H_
