// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embedding limit to [[ScanEdges]]
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
// |    ScanEdges      |
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
// |    ScanEdges      |
// |     (limit=3)     |
// +---------+---------+

class PushLimitDownScanEdgesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownScanEdgesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
