/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef NGQL_PLANNERS_SUBGRAPHPLANNER_H
#define NGQL_PLANNERS_SUBGRAPHPLANNER_H

#include <stdint.h>  // for uint32_t

#include <memory>  // for unique_ptr
#include <string>  // for string
#include <vector>  // for vector

#include "common/base/StatusOr.h"  // for StatusOr
#include "graph/context/QueryContext.h"
#include "graph/context/ast/AstContext.h"  // for AstContext
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"  // for Planner
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"  // for EdgeProp
#include "parser/Sentence.h"                   // for Sentence, Sentence::Kind

namespace nebula {
class Expression;
namespace graph {
struct SubPlan;
struct SubgraphContext;
}  // namespace graph

class Expression;

namespace graph {
struct SubPlan;
struct SubgraphContext;

class SubgraphPlanner final : public Planner {
 public:
  using EdgeProp = storage::cpp2::EdgeProp;

  static std::unique_ptr<SubgraphPlanner> make() {
    return std::unique_ptr<SubgraphPlanner>(new SubgraphPlanner());
  }

  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kGetSubgraph;
  }

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

  StatusOr<SubPlan> zeroStep(SubPlan& startVidPlan, const std::string& input);

  StatusOr<SubPlan> nSteps(SubPlan& startVidPlan, const std::string& input);

  StatusOr<std::unique_ptr<std::vector<EdgeProp>>> buildEdgeProps();

  Expression* loopCondition(uint32_t steps, const std::string& var);

 private:
  SubgraphPlanner() = default;

  SubgraphContext* subgraphCtx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  //  NGQL_PLANNERS_SUBGRAPHPLANNER_H
