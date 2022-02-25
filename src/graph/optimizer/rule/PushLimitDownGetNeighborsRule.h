/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNGETNEIGHBORSRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNGETNEIGHBORSRULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

/*
  Push down the filter items
  Required conditions:
   1. Match the pattern
  Benefits:
   1. Filter data early to optimize performance

  Tranformation:
  Before:

  +-------------+-------------+
  |           Filter          |
  |   ($^.age>3 and $$.age<4) |
  +-------------+-------------+
                |
      +---------+---------+
      |   GetNeighbors    |
      +---------+---------+

  After:

  +--------+--------+
  |      Filter     |
  |     ($$.age<4)  |
  +--------+--------+
           |
 +---------+---------+
 |    GetNeighbors   |
 |     ($^.age>3)    |
 +---------+---------+

*/
class PushLimitDownGetNeighborsRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownGetNeighborsRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
