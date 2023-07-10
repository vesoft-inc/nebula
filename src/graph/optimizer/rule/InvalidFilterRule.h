/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef GRAPH_OPTIMIZER_INVALIDFILTERRULE_H_
#define GRAPH_OPTIMIZER_INVALIDFILTERRULE_H_

#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/OptimizerUtils.h"

namespace nebula {
namespace opt {

class OptContext;

class InvalidFilterRule final : public OptRule {
 public:
  const Pattern& pattern() const override;

  bool match(OptContext* ctx, const MatchedResult& matched) const override;
  StatusOr<TransformResult> transform(OptContext* ctx, const MatchedResult& matched) const override;
  std::string toString() const override;

 private:
  static std::unique_ptr<OptRule> kInstance;

  InvalidFilterRule();

  bool isAlwaysFalse(OptContext* ctx, const Expression* filter) const;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_INVALIDFILTERRULE_H_
