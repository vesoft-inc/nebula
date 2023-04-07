/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

/*
 * Before:
 * Filter(all(i in e where i.likeness > 78))
 *        |
 *     Traverse
 *
 * After :
 *     Traverse(eFilter_: *.likeness > 78)
 */
class EmbedEdgeAllPredIntoTraverseRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  EmbedEdgeAllPredIntoTraverseRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
