/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDDEDUPRULE_H_
#define GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDDEDUPRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Merge [[Dedup]] and [[GetNeighbors]] node
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Delete unnecessary node
//
//  Tranformation:
//  Before:
//
//  +------+-------+
//  | GetNeighbors |
//  +------+-------+
//         |
//  +------+------+
//  |    Dedup    |
//  +------+------+
//
//  After:
//
//  +------+-------+
//  | GetNeighbors |
//  | (dedup=true) |
//  +------+-------+

class MergeGetNbrsAndDedupRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  MergeGetNbrsAndDedupRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDDEDUPRULE_H_
