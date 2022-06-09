/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_GETEDGESTRANSFORMRULE_H
#define GRAPH_OPTIMIZER_RULE_GETEDGESTRANSFORMRULE_H

#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::ScanEdges;
using nebula::graph::Traverse;

namespace nebula {

namespace opt {

//  Convert [[ScanVertices]] to [[ScanEdges]] in certain cases
//  Required conditions:
//   1. Match the pattern
//  Benefits:
//   1. Avoid doing Traverse to optimize performance
//  Quey example:
//   1. match ()-[e]->() return e limit 3
//
//  Tranformation:
//  Before:
// +---------+---------+
// |      Project      |
// +---------+---------+
//           |
// +---------+---------+
// |       Limit       |
// +---------+---------+
//           |
// +---------+---------+
// |      Traverse     |
// +---------+---------+
//           |
// +---------+---------+
// |    ScanVertices   |
// +---------+---------+
//
//  After:
// +---------+---------+
// |      Project      |
// +---------+---------+
//           |
// +---------+---------+
// |       Limit       |
// +---------+---------+
//           |
// +---------+---------+
// |      Project      |
// +---------+---------+
//           |
// +---------+---------+
// |      ScanEdges    |
// +---------+---------+

class GetEdgesTransformRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

  static graph::ScanEdges *traverseToScanEdges(const graph::Traverse *traverse,
                                               const int64_t limit_count = -1) {
    const auto *edgeProps = traverse->edgeProps();
    if (edgeProps == nullptr) {
      return nullptr;
    }
    for (std::size_t i = 0; i < edgeProps->size(); i++) {
      auto type = (*edgeProps)[i].get_type();
      for (std::size_t j = i + 1; j < edgeProps->size(); j++) {
        if (type == -((*edgeProps)[j].get_type())) {
          // Don't support to retrieve edges of the inbound/outbound together
          return nullptr;
        }
      }
    }
    auto scanEdges = ScanEdges::make(
        traverse->qctx(),
        nullptr,
        traverse->space(),
        edgeProps == nullptr ? nullptr
                             : std::make_unique<std::vector<storage::cpp2::EdgeProp>>(*edgeProps),
        nullptr,
        traverse->dedup(),
        limit_count,
        {},
        traverse->filter() == nullptr ? nullptr : traverse->filter()->clone());
    return scanEdges;
  }

  static graph::Project *projectEdges(graph::QueryContext *qctx,
                                      PlanNode *input,
                                      const std::string &colName) {
    auto *yieldColumns = qctx->objPool()->makeAndAdd<YieldColumns>();
    auto *edgeExpr = EdgeExpression::make(qctx->objPool());
    auto *listEdgeExpr = ListExpression::make(qctx->objPool());
    listEdgeExpr->setItems({edgeExpr});
    yieldColumns->addColumn(new YieldColumn(listEdgeExpr, colName));
    auto project = Project::make(qctx, input, yieldColumns);
    project->setColNames({colName});
    return project;
  }

 private:
  GetEdgesTransformRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
