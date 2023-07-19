/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNSCANVERTICESRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNSCANVERTICESRULE_H

#include <memory>

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push down the filter items
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Filter data early to optimize performance
//
//  Transformation:
//  Before:
//
//  +-------------+-------------+
//  |           Filter          |
//  |($p1>3 and $p2<4 and $p1<9)|
//  +-------------+-------------+
//                |
//      +---------+---------+
//      |   ScanVertices    |
//      +---------+---------+
//
//  After:
//
//  +--------+--------+
//  |      Filter     |
//  |($p1>3 and $p1<9)|
//  +--------+--------+
//           |
// +---------+---------+
// |    ScanVertices   |
// |      ($p2<4)      |
// +---------+---------+

class PushFilterDownScanVerticesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownScanVerticesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
