// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_PLANNER_PLAN_ALGO_H_
#define GRAPH_PLANNER_PLAN_ALGO_H_

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

class FindPath : public BinaryInputNode {
 public:
  static FindPath* make(QueryContext* qctx,
                        PlanNode* left,
                        PlanNode* right,
                        bool shortest,
                        bool noLoop,
                        size_t steps) {
    return qctx->objPool()->add(new FindPath(qctx, left, right, shortest, noLoop, steps));
  }

  size_t steps() const {
    return steps_;
  }

  bool isShortest() const {
    return shortest_;
  }

  bool noLoop() const {
    return noLoop_;
  }

  std::string leftVidVar() const {
    return leftVidVar_;
  }

  void setLeftVidVar(const std::string& var) {
    leftVidVar_ = var;
  }

  void setRightVidVar(const std::string& var) {
    rightVidVar_ = var;
  }

  std::string rightVidVar() const {
    return rightVidVar_;
  }

  void setTerminationVar(const std::string& var) {
    terminationVar_ = var;
  }

  std::string terminationVar() const {
    return terminationVar_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  FindPath(
      QueryContext* qctx, PlanNode* left, PlanNode* right, bool shortest, bool noLoop, size_t steps)
      : BinaryInputNode(qctx, Kind::kFindPath, left, right),
        shortest_(shortest),
        noLoop_(noLoop),
        steps_(steps) {}

 private:
  bool shortest_{false};
  bool noLoop_{false};
  std::string leftVidVar_;
  std::string rightVidVar_;
  size_t steps_{0};
  std::string terminationVar_;
};

class CartesianProduct final : public SingleDependencyNode {
 public:
  static CartesianProduct* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new CartesianProduct(qctx, input));
  }

  Status addVar(std::string varName);

  std::vector<std::string> inputVars() const;

  std::vector<std::vector<std::string>> allColNames() const {
    return allColNames_;
  }
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  CartesianProduct(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kCartesianProduct, input) {}

  std::vector<std::vector<std::string>> allColNames_;
};

class Subgraph final : public SingleInputNode {
 public:
  static Subgraph* make(QueryContext* qctx,
                        PlanNode* input,
                        const std::string& resultVar,
                        const std::string& currentStepVar,
                        uint32_t steps) {
    return qctx->objPool()->add(new Subgraph(qctx, input, resultVar, currentStepVar, steps));
  }

  const std::string& resultVar() const {
    return resultVar_;
  }

  const std::string& currentStepVar() const {
    return currentStepVar_;
  }

  uint32_t steps() const {
    return steps_;
  }

  const std::unordered_set<EdgeType> biDirectEdgeTypes() const {
    return biDirectEdgeTypes_;
  }

  void setBiDirectEdgeTypes(std::unordered_set<EdgeType> edgeTypes) {
    biDirectEdgeTypes_ = std::move(edgeTypes);
  }

 private:
  Subgraph(QueryContext* qctx,
           PlanNode* input,
           const std::string& resultVar,
           const std::string& currentStepVar,
           uint32_t steps)
      : SingleInputNode(qctx, Kind::kSubgraph, input),
        resultVar_(resultVar),
        currentStepVar_(currentStepVar),
        steps_(steps) {}

  std::string resultVar_;
  std::string currentStepVar_;
  uint32_t steps_;
  std::unordered_set<EdgeType> biDirectEdgeTypes_;
};

class BiCartesianProduct final : public BinaryInputNode {
 public:
  static BiCartesianProduct* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
    return qctx->objPool()->add(new BiCartesianProduct(qctx, left, right));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  BiCartesianProduct(QueryContext* qctx, PlanNode* left, PlanNode* right);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLAN_ALGO_H_
