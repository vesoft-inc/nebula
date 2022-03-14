/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNPROJECTRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNPROJECTRULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push [[Limit]] down [[Project]]
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Limit data early to optimize performance
//
//  Tranformation:
//  Before:
//
//  +--------+--------+
//  |      Limit      |
//  |    (limit=3)    |
//  +--------+--------+
//           |
// +---------+---------+
// |      Project      |
// +---------+---------+
//
//  After:
//
//
// +---------+---------+
// |      Project      |
// +---------+---------+
//           |
//  +--------+--------+
//  |      Limit      |
//  |    (limit=3)    |
//  +--------+--------+

class PushLimitDownProjectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownProjectRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
