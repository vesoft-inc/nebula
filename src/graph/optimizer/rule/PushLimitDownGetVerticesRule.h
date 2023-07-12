/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNGETVERTICESRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNGETVERTICESRULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embedding limit to [[GetVertices]]
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Limit data early to optimize performance
//
//  Transformation:
//  Before:
//
//  +--------+--------+
//  |      Limit      |
//  |    (limit=3)    |
//  +--------+--------+
//           |
// +---------+---------+
// |    GetVertcies    |
// +---------+---------+
//
//  After:
//
//  +--------+--------+
//  |      Limit      |
//  |    (limit=3)    |
//  +--------+--------+
//           |
// +---------+---------+
// |    GetVertices    |
// |     (limit=3)     |
// +---------+---------+

class PushLimitDownGetVerticesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownGetVerticesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif
