/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNVECTORINDEXSCAN_H_
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNVECTORINDEXSCAN_H_

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
// +----------------------+
// | GetVertices/GetEdges |
// |    (limit=3)         |
// +---------+------------+
//           |
// +---------+---------+
// | VectorIndexScan   |
// +---------+---------+
//
//  After:
//
// +----------------------+
// | GetVertices/GetEdges |
// |    (limit=3)         |
// +---------+------------+
//           |
// +---------+---------+
// | VectorIndexScan   |
// |     (limit=3)     |
// +---------+---------+

class PushLimitDownVectorIndexScanRule final : public OptRule {
 public:
  const Pattern& pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext* ctx,
                                             const MatchedResult& matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownVectorIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif 