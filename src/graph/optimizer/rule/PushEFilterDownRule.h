// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

class PushEFilterDownRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushEFilterDownRule();

  // Rewrite `*.prop` TO `edge1.prop OR edge2.prop OR ...`
  static Expression *rewriteStarEdge(Expression *expr,
                                     GraphSpaceID spaceId,
                                     const std::vector<storage::cpp2::EdgeProp> &edges,
                                     meta::SchemaManager *schemaMng,
                                     bool isBothDirection,
                                     ObjectPool *pool);
  static Expression *rewriteStarEdge(const PropertyExpression *exp,
                                     GraphSpaceID spaceId,
                                     const storage::cpp2::EdgeProp &edge,
                                     meta::SchemaManager *schemaMng,
                                     ObjectPool *pool);

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
