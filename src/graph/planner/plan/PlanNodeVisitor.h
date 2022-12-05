/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef PLAN_PLANNODEVISITOR_H_
#define PLAN_PLANNODEVISITOR_H_

#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

class PlanNodeVisitor {
 public:
  virtual ~PlanNodeVisitor() = default;

  virtual void visit(PlanNode *node) = 0;
  virtual void visit(Filter *node) = 0;
  virtual void visit(Project *node) = 0;
  virtual void visit(Aggregate *node) = 0;
  virtual void visit(Traverse *node) = 0;
  virtual void visit(ScanEdges *node) = 0;
  virtual void visit(AppendVertices *node) = 0;
  virtual void visit(HashJoin *node) = 0;
  virtual void visit(Union *node) = 0;
  virtual void visit(Unwind *node) = 0;
  virtual void visit(CrossJoin *node) = 0;
};

}  // namespace graph
}  // namespace nebula

#endif  // PLAN_PLANNODEVISITOR_H_
