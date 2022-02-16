/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHSTEPLIMITDOWNGETNEIGHBORSRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHSTEPLIMITDOWNGETNEIGHBORSRULE_H

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/base/StatusOr.h"     // for StatusOr
#include "graph/optimizer/OptRule.h"  // for OptRule, MatchedResult (p...

namespace nebula {
namespace opt {
class OptContext;

class OptContext;

class PushStepLimitDownGetNeighborsRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushStepLimitDownGetNeighborsRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
