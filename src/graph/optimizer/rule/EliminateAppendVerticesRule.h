// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

// Eliminate useless AppendVertices node
class EliminateAppendVerticesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  EliminateAppendVerticesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
