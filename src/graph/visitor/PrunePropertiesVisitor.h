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
  // \param node, the current node to visit
  // \param used, whether properties in current node are used
  // void visitCurrent(PlanNode *node, bool used = false);

  void visit(Filter *node) override;
  // \param node, the current node to visit
  // \param used, whether properties in current node are used
  void visitCurrent(Filter *node);

  void visit(Project *node) override;
  // \param node, the current node to visit
  // \param used, whether properties in current node are used
  void visitCurrent(Project *node);

  void visit(Aggregate *node) override;
  // \param node, the current node to visit
  // \param used, whether properties in current node are used
  void visitCurrent(Aggregate *node);

  void visit(Traverse *node) override;
  // \param node, the current node to visit
  // \param used, whether properties in current node are used
  void visitCurrent(Traverse *node);
  // prune properties in Traverse according to the used properties collected previous
  void pruneCurrent(Traverse *node);

  void visit(ScanEdges *node) override;

  void pruneCurrent(ScanEdges *node);

  void visit(AppendVertices *node) override;
  // \param node, the current node to visit
  // \param used, whether properties in current node are used
  void visitCurrent(AppendVertices *node);
  // prune properties in AppendVertices according to the used properties collected previous
  void pruneCurrent(AppendVertices *node);

  void visit(HashJoin *node) override;

  void visit(Union *node) override;
  void visit(CrossJoin *node) override;

  void visit(Unwind *node) override;
  void visitCurrent(Unwind *node);

 private:
  Status depsPruneProperties(std::vector<const PlanNode *> &dependencies);
  Status pruneBinaryBranch(std::vector<const PlanNode *> &dependencies);
  Status extractPropsFromExpr(const Expression *expr, const std::string &entityAlias = "");

  PropertyTracker &propsUsed_;
  QueryContext *qctx_;
  GraphSpaceID spaceID_;
  Status status_;
  bool rootNode_{true};
  const int unknownType_ = 0;
  std::unordered_set<PlanNode *> visitedPlanNode_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_PRUNEPROPERTIES_VISITOR_H_
