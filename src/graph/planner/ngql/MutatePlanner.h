/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NGQL_PLANNERS_MUTATE_PLANNER_H
#define NGQL_PLANNERS_NUTATE_PLANNER_H

#include "graph/context/QueryContext.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

class InsertVerticesPlanner final : public Planner {
 public:
  static std::unique_ptr<InsertVerticesPlanner> make() {
    return std::unique_ptr<InsertVerticesPlanner>(new InsertVerticesPlanner());
  }

  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kInsertVertices;
  }

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

 private:
  InsertVerticesPlanner() = default;

  InsertVerticesContext* insertCtx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  //  NGQL_PLANNERS_SUBGRAPHPLANNER_H
