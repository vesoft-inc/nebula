/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_REMOVENOOPPROJECTRULE_H_
#define GRAPH_OPTIMIZER_RULE_REMOVENOOPPROJECTRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Remove [[Project]] without any Expression computing and alias operation
//  Required conditions:
//   1. Match the pattern
//   2. The [[Project]] has the same column names as the previous node
//  Benefits:
//   1. Remove unnecessary Project node
//
//  Tranformation:
//  Before:
//
// +---------+---------+
// |      Project      |
// +---------+---------+
//
//  After:
//  // Remove Project node

class RemoveNoopProjectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  RemoveNoopProjectRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_REMOVENOOPPROJECTRULE_H_
