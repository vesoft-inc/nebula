/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNAPPENDVERTICES_H_
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNAPPENDVERTICES_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

class PushFilterDownAppendVerticesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownAppendVerticesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNAPPENDVERTICES_H_
