/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {
// Before:
//      HashLeftJoin({id(v)}, {id(v)})
//          |           |
//         ...       Project
//          |           |
//  AppendVertices(v) AppendVertices(v)
//          |           |
//         ...       Traverse(e)
//
//  After:
//     HashLeftJoin({id(v)}, {$-.v})
//         |            |
//        ...     Project(..., none_direct_dst(e) AS v)
//         |            |
//  AppendVertices(v) Traverse(e)
//         |
//        ...
//
class OptimizeLeftJoinPredicateRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *qctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  OptimizeLeftJoinPredicateRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
