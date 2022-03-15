/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLAN_ALGO_H_
#define GRAPH_PLANNER_PLAN_ALGO_H_

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
class ProduceSemiShortestPath : public SingleInputNode {
 public:
  static ProduceSemiShortestPath* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ProduceSemiShortestPath(qctx, input));
  }

 private:
  ProduceSemiShortestPath(QueryContext* qctx, PlanNode* input)
      : SingleInputNode(qctx, Kind::kProduceSemiShortestPath, input) {}
};

class BFSShortestPath : public SingleInputNode {
 public:
  static BFSShortestPath* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new BFSShortestPath(qctx, input));
  }

 private:
  BFSShortestPath(QueryContext* qctx, PlanNode* input)
      : SingleInputNode(qctx, Kind::kBFSShortest, input) {}
};

class ConjunctPath : public BinaryInputNode {
 public:
  enum class PathKind : uint8_t {
    kBiBFS,
    kBiDijkstra,
    kFloyd,
    kAllPaths,
  };

  static ConjunctPath* make(
      QueryContext* qctx, PlanNode* left, PlanNode* right, PathKind pathKind, size_t steps) {
    return qctx->objPool()->add(new ConjunctPath(qctx, left, right, pathKind, steps));
  }

  PathKind pathKind() const {
    return pathKind_;
  }

  size_t steps() const {
    return steps_;
  }

  void setConditionalVar(std::string varName) {
    conditionalVar_ = std::move(varName);
  }

  std::string conditionalVar() const {
    return conditionalVar_;
  }

  bool noLoop() const {
    return noLoop_;
  }

  void setNoLoop(bool noLoop) {
    noLoop_ = noLoop;
  }
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  ConjunctPath(QueryContext* qctx, PlanNode* left, PlanNode* right, PathKind pathKind, size_t steps)
      : BinaryInputNode(qctx, Kind::kConjunctPath, left, right) {
    pathKind_ = pathKind;
    steps_ = steps;
  }

  PathKind pathKind_;
  size_t steps_{0};
  std::string conditionalVar_;
  bool noLoop_;
};

class ProduceAllPaths final : public SingleInputNode {
 public:
  static ProduceAllPaths* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ProduceAllPaths(qctx, input));
  }

  bool noLoop() const {
    return noLoop_;
  }

  void setNoLoop(bool noLoop) {
    noLoop_ = noLoop;
  }
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  ProduceAllPaths(QueryContext* qctx, PlanNode* input)
      : SingleInputNode(qctx, Kind::kProduceAllPaths, input) {}

 private:
  bool noLoop_{false};
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
