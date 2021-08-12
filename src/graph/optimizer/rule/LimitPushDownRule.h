/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_OPTIMIZER_RULE_LIMITPUSHDOWN_H_
#define GRAPH_OPTIMIZER_RULE_LIMITPUSHDOWN_H_

#include <memory>

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

class LimitPushDownRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  LimitPushDownRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_LIMITPUSHDOWN_H_
