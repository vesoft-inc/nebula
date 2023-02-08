/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNHASHLEFTJOINRULE_H_
#define GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNHASHLEFTJOINRULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  Push down the filter items from the left subplan of [[HashLeftJoin]]
class PushFilterDownHashLeftJoinRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *octx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownHashLeftJoinRule();
  static OptGroup *pushFilterDownChild(OptContext *octx,
                                       const MatchedResult &child,
                                       const Expression *condition,
                                       Expression **unpickedFilter);

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_PUSHFILTERDOWNHASHLEFTJOINRULE_H_
