/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNCROSSJOINRULE_H_
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNCROSSJOINRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push down the filter items into the child sub-plan of [[CrossJoin]]
class PushFilterDownCrossJoinRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *octx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownCrossJoinRule();
  static OptGroup *pushFilterDownChild(OptContext *octx,
                                       const MatchedResult &child,
                                       const Expression *condition,
                                       Expression **unpickedFilter);

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNCROSSJOINRULE_H_
