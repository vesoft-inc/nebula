/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNSCANEDGESAPPENDVERTICESRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNSCANEDGESAPPENDVERTICESRULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Embedding limit to [[ScanEdges]]
//  Required conditions:
//   1. Match the pattern
//   2. All filters of [[AppendVertices]] must be nullptr
//  Benefits:
//   1. Limit data early to optimize performance
//
//  Tranformation:
//  Before:
//
//  +--------+--------+
//  |      Limit      |
//  |    (limit=3)    |
//  +--------+--------+
//           |
// +---------+---------+
// |   AppendVertices  |
// +---------+---------+
//           |
// +---------+---------+
// |      ScanEdges    |
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
// |   AppendVertices  |
// +---------+---------+
//           |
// +---------+---------+
// |     ScanEdges     |
// |     (limit=3)     |
// +---------+---------+

class PushLimitDownScanEdgesAppendVerticesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownScanEdgesAppendVerticesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
