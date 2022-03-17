/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_MERGEGETVERTICESANDPROJECTRULE_H_
#define GRAPH_OPTIMIZER_RULE_MERGEGETVERTICESANDPROJECTRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Merge [[Project]] and [[GetVertices]] node
//  Required conditions:
//   1. Match the pattern
//   2. The projection must project only one column which expression will be assigned to the src
//  expression of GetVertices Benefits:
//   1. Delete unnecessary node
//
//  Tranformation:
//  Before:
//
//  +------+-------+
//  | GetVertices |
//  | (src:$-.vage)|
//  +------+-------+
//         |
//  +-------+-------+
//  |    Project    |
//  |(v.age AS vage)|
//  +-------+-------+
//
//  After:
//
//  +------+-------+
//  | GetVertices |
//  | (src:v.age)  |
//  +------+-------+

class MergeGetVerticesAndProjectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;
  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  MergeGetVerticesAndProjectRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_MERGEGETVERTICESANDPROJECTRULE_H_
