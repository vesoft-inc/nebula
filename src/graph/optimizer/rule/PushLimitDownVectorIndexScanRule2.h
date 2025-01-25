/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNVECTORINDEXSCAN2_H_
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNVECTORINDEXSCAN2_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embedding limit to [[VectorIndexScan]]
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Limit data early to optimize performance
//
//  Transformation:
//  Before:
//
// Limit (count=3, offset=1)
//   `- HashInnerJoin
//       |- Project
//       |   `- GetVertices/GetEdges
//       |       `- Argument
//       `- VectorIndexScan
//
//  After:
//
// HashInnerJoin
//  |- Project
//  |   `- GetVertices/GetEdges
//  |       `- Argument
//  `-VectorIndexScan (limit=4, offset=1)
//

class PushLimitDownVectorIndexScanRule2 final : public OptRule {
 public:
  const Pattern& pattern() const override;

  bool match(OptContext* ctx, const MatchedResult& matched) const override;

  StatusOr<OptRule::TransformResult> transform(OptContext* ctx,
                                             const MatchedResult& matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownVectorIndexScanRule2();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif 