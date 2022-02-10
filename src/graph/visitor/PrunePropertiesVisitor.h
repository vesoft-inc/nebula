/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_PRUNEPROPERTIES_VISITOR_H_
#define GRAPH_VISITOR_PRUNEPROPERTIES_VISITOR_H_

#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/PlanNodeVisitor.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/PropertyTrackerVisitor.h"

namespace nebula {
namespace graph {

class PrunePropertiesVisitor final : public PlanNodeVisitor {
 public:
  PrunePropertiesVisitor(PropertyTracker &propsUsed,
                         graph::QueryContext *qctx,
                         GraphSpaceID spaceID);

  bool ok() const {
    return status_.ok();
  }

  const Status &status() const {
    return status_;
  }

  void visit(PlanNode *node) override;
  void visit(Filter *node) override;
  void visit(Project *node) override;
  void visit(Aggregate *node) override;
  void visit(Traverse *node) override;
  void visit(AppendVertices *node) override;
  void visit(BiJoin *node) override;

 private:
  Status depsPruneProperties(std::vector<const PlanNode *> &dependencies);
  Status extractPropsFromExpr(const Expression *expr, const std::string &entityAlias = "");

  PropertyTracker &propsUsed_;
  QueryContext *qctx_;
  GraphSpaceID spaceID_;
  Status status_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_PRUNEPROPERTIES_VISITOR_H_
