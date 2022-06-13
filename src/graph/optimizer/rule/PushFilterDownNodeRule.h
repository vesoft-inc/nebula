// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include <memory>

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push down the vertex filter of Traverse/AppendVertices

class PushFilterDownNodeRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

 private:
  PushFilterDownNodeRule();

  static std::unique_ptr<OptRule> kInstance;

  static const std::initializer_list<graph::PlanNode::Kind> kNodes;
};

}  // namespace opt
}  // namespace nebula
