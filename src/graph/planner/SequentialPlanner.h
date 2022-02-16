/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLANNERS_SEQUENTIALPLANNER_H_
#define GRAPH_PLANNER_PLANNERS_SEQUENTIALPLANNER_H_

#include <memory>  // for unique_ptr

#include "common/base/StatusOr.h"  // for StatusOr
#include "graph/context/QueryContext.h"
#include "graph/planner/Planner.h"  // for Planner
#include "graph/validator/Validator.h"
#include "parser/Sentence.h"  // for Sentence, Sentence::Kind

namespace nebula {
namespace graph {
class QueryContext;
class Validator;
struct AstContext;
struct SubPlan;

class QueryContext;
class Validator;
struct AstContext;
struct SubPlan;

class SequentialPlanner final : public Planner {
 public:
  static std::unique_ptr<SequentialPlanner> make() {
    return std::unique_ptr<SequentialPlanner>(new SequentialPlanner());
  }

  static bool match(AstContext* astCtx);

  /**
   * Each sentence would be converted to a sub-plan, and they would
   * be cascaded together into a complete execution plan.
   */
  StatusOr<SubPlan> transform(AstContext* astCtx) override;

  void ifBuildDataCollect(SubPlan& subPlan, QueryContext* qctx);

  void rmLeftTailStartNode(Validator* validator, Sentence::Kind appendPlanKind);

 private:
  SequentialPlanner() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLANNERS_SEQUENTIALPLANNER_H_
