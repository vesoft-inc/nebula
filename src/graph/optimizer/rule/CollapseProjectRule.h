/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_COLLAPSEPROJECTRULE_H_
#define GRAPH_OPTIMIZER_RULE_COLLAPSEPROJECTRULE_H_

#include "graph/optimizer/OptRule.h"

DECLARE_bool(enable_optimizer_collapse_project_rule);

namespace nebula {
namespace opt {

//  Combines two [[Project]] nodes into one
//  Required conditions:
//   1. Match the pattern
//   2. Expressions between nodes cannot be referenced more than once
//  Benefits:
//   1. reduce the copy of memory between nodes
//   2. reduces expression overhead in some cases(similar to column pruning)
//
//  Tranformation:
//  Before:
//
//         +------------+------------+
//         |       Project           |
//         | ($A1+1 AS A2,$B1 AS B2) |
//         +------------+------------+
//                      |
//  +-------------------+-------------------+
//  |                Project                |
//  | ($v.age+1 AS A1,$v AS B1,$n.age AS C1)|
//  +-------------------+-------------------+
//
//  After:
//
//       +--------------+--------------+
//       |           Project           |
//       | ($v.age+1+1 AS A2,$v AS B2) |
//       +--------------+--------------+
//

class CollapseProjectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  CollapseProjectRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_COLLAPSEPROJECTRULE_H_
