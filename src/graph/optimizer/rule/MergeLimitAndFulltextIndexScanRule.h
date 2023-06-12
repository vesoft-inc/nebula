/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_MERGELIMITANDFULLTEXTINDEXSCAN_H_
#define GRAPH_OPTIMIZER_RULE_MERGELIMITANDFULLTEXTINDEXSCAN_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embedding limit/offset to [[FulltextIndexScan]]
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Limit data early to optimize performance
//
//  Transformation:
//  Before:
//
//  +--------+------------+
//  |      Limit          |
//  | (count=3, offset=1) |
//  +--------+------------+
//           |
// +---------+------------+
// | GetVertices/GetEdges |
// |    (limit=4)         |
// +---------+------------+
//           |
// +---------+---------+
// | FulltextIndexScan |
// +---------+---------+
//
//  After:
//
// +---------+------------+
// | GetVertices/GetEdges |
// |    (limit=4)         |
// +---------+------------+
//           |
// +---------+-----------+
// | FulltextIndexScan   |
// | (limit=4, offset=1) |
// +---------+-----------+

class MergeLimitAndFulltextIndexScanRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  MergeLimitAndFulltextIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif
