/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {

namespace graph {
class ScanEdges;
class Project;
class Traverse;
class PlanNode;
}  // namespace graph

namespace opt {

//  Convert [[ScanVertices]] to [[ScanEdges]] in certain cases
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Avoid doing Traverse to optimize performance
//  Query example:
//   1. match ()-[e]->() return e limit 1
//
//  Transformation:
//  Before:
// +---------+---------+
// |      Project      |
// +---------+---------+
//           |
// +---------+---------+
// |       Limit       |
// +---------+---------+
//           |
// +---------+---------+
// |   AppendVertices  |
// +---------+---------+
//           |
// +---------+---------+
// |      Traverse     |
// +---------+---------+
//           |
// +---------+---------+
// |    ScanVertices   |
// +---------+---------+
//
//  After:
// +---------+---------+
// |      Project      |
// +---------+---------+
//           |
// +---------+---------+
// |       Limit       |
// +---------+---------+
//           |
// +---------+---------+
// |   AppendVertices  |
// +---------+---------+
//           |
// +---------+---------+
// |      Project      |
// +---------+---------+
//           |
// +---------+---------+
// |      ScanEdges    |
// +---------+---------+

class GetEdgesTransformAppendVerticesLimitRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  GetEdgesTransformAppendVerticesLimitRule();

  static graph::Project *projectEdges(graph::QueryContext *qctx,
                                      graph::PlanNode *input,
                                      const std::string &colName);

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
