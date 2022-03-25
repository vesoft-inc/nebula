/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef GRAPH_PLANNER_NGQL_MAINTAINPLANNER_H
#define GRAPH_PLANNER_NGQL_MAINTAINPLANNER_H

#include "common/base/Base.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

struct AstContext;

class CreateTagPlanner final : public Planner {
 public:
  static std::unique_ptr<CreateTagPlanner> make() {
    return std::unique_ptr<CreateTagPlanner>(new CreateTagPlanner());
  }
  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kCreateTag;
  }
  StatusOr<SubPlan> transform(AstContext* astCtx) override;
};

class CreateEdgePlanner final : public Planner {
 public:
  static std::unique_ptr<CreateEdgePlanner> make() {
    return std::unique_ptr<CreateEdgePlanner>(new CreateEdgePlanner());
  }
  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kCreateEdge;
  }
  StatusOr<SubPlan> transform(AstContext* astCtx) override;
};

class AlterTagPlanner final : public Planner {
 public:
  static std::unique_ptr<AlterTagPlanner> make() {
    return std::unique_ptr<AlterTagPlanner>(new AlterTagPlanner());
  }
  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kAlterTag;
  }
  StatusOr<SubPlan> transform(AstContext* astCtx) override;
};

class AlterEdgePlanner final : public Planner {
 public:
  static std::unique_ptr<AlterEdgePlanner> make() {
    return std::unique_ptr<AlterEdgePlanner>(new AlterEdgePlanner());
  }
  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kAlterEdge;
  }
  StatusOr<SubPlan> transform(AstContext* astCtx) override;
};

}  // namespace graph
}  // namespace nebula
#endif
