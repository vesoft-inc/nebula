/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDPROJECTRULE_H_
#define GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDPROJECTRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

/*
  Embedding dedup factor into [[GetNeighbor]] node
  Required conditions:
   1. Match the pattern
  Benefits:
   1. Delete unnecessary node

  Tranformation:
  Before:

  +------+------+
  |    Dedup    |
  +------+------+
         |
  +------+-------+
  | GetNeighbors |
  | (dedup=false)|
  +------+-------+

  After:

  +------+-------+
  | GetNeighbors |
  | (dedup=true) |
  +------+-------+

*/
class MergeGetNbrsAndProjectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;
  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  MergeGetNbrsAndProjectRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDPROJECTRULE_H_
