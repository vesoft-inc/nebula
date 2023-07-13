/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Eliminate useless [[Filter(false)]] node
//  Required conditions:
//   1. Match the pattern
//   2. Filter condition is false/null
//  Benefits:
//   1. Delete unnecessary node
//
//  Transformation:
//  Before:
//
//  +------+--------+
//  | Filter(false) |
//  +------+--------+
//         |
//  +------+------+
//  |    .....    |
//  +------+------+
//
//  After:
//
//  +------+------+
//  |   Value     |
//  +------+------+
//  |    Start    |
//  +------+------+

class EliminateFilterRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  EliminateFilterRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
