/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERTHROUGHAPPENDVERTICES_H_
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERTHROUGHAPPENDVERTICES_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

/*
 * Before:
 *    Filter(e.likeness > 78 and v.prop > 40)
 *        |
 *   AppendVertices(v)
 *
 * After :
 *   Filter(v.prop > 40)
 *        |
 *   AppendVertices(v)
 *        |
 *   Filter(e.likeness > 78)
 *
 */

class PushFilterThroughAppendVerticesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterThroughAppendVerticesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERTHROUGHAPPENDVERTICES_H_
