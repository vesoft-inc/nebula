/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Eliminate useless [[DataCollect]] node
//  Required conditions:
//   1. Match the pattern
//   2. DataCollect::DCKind is `kRowBasedMove`
//  Benefits:
//   1. Delete unnecessary node
//
//  Tranformation:
//  Before:
//
//  +------+------+
//  | DataCollect |
//  +------+------+
//         |
//  +------+------+
//  |   Project   |
//  +------+------+
//
//  After:
//
//  +------+------+
//  |   Project   |
//  +------+------+

class EliminateRowCollectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  EliminateRowCollectRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
