/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NGQL_PLANNERS_MUTATE_PLANNER_H
#define NGQL_PLANNERS_MUTATE_PLANNER_H

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

class InsertEdgesPlanner final : public Planner {
 public:
  static std::unique_ptr<InsertEdgesPlanner> make() {
    return std::unique_ptr<InsertEdgesPlanner>(new InsertEdgesPlanner());
  }

  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kInsertEdges;
  }

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

 private:
  InsertEdgesPlanner() = default;

  InsertEdgesContext* insertCtx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  //  NGQL_PLANNERS_MUTATE_PLANNER_H
