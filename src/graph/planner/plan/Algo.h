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
class MultiShortestPath : public BinaryInputNode {
 public:
  static MultiShortestPath* make(QueryContext* qctx,
                                 PlanNode* left,
                                 PlanNode* right,
                                 size_t steps) {
    return qctx->objPool()->makeAndAdd<MultiShortestPath>(qctx, left, right, steps);
  }

  size_t steps() const {
    return steps_;
  }

  std::string leftVidVar() const {
    return leftVidVar_;
  }

  std::string rightVidVar() const {
    return rightVidVar_;
  }

  std::string terminationVar() const {
    return terminationVar_;
  }

  void setLeftVidVar(const std::string& var) {
    leftVidVar_ = var;
  }

  void setRightVidVar(const std::string& var) {
    rightVidVar_ = var;
  }

  void setTerminationVar(const std::string& var) {
    terminationVar_ = var;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  MultiShortestPath(QueryContext* qctx, PlanNode* left, PlanNode* right, size_t steps)
      : BinaryInputNode(qctx, Kind::kMultiShortestPath, left, right), steps_(steps) {}

 private:
  size_t steps_{0};
  std::string leftVidVar_;
  std::string rightVidVar_;
  std::string terminationVar_;
};

class BFSShortestPath : public BinaryInputNode {
 public:
  static BFSShortestPath* make(QueryContext* qctx, PlanNode* left, PlanNode* right, size_t steps) {
    return qctx->objPool()->makeAndAdd<BFSShortestPath>(qctx, left, right, steps);
  }

  size_t steps() const {
    return steps_;
  }

  std::string leftVidVar() const {
    return leftVidVar_;
  }

  std::string rightVidVar() const {
    return rightVidVar_;
  }

  std::string terminateEarlyVar() const {
    return terminateEarlyVar_;
  }

  void setLeftVidVar(const std::string& var) {
    leftVidVar_ = var;
  }

  void setRightVidVar(const std::string& var) {
    rightVidVar_ = var;
  }

  void setTerminateEarlyVar(const std::string& var) {
    terminateEarlyVar_ = var;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  BFSShortestPath(QueryContext* qctx, PlanNode* left, PlanNode* right, size_t steps)
      : BinaryInputNode(qctx, Kind::kBFSShortest, left, right), steps_(steps) {}

 private:
  std::string leftVidVar_;
  std::string rightVidVar_;
  std::string terminateEarlyVar_;
  size_t steps_{0};
};

class ProduceAllPaths final : public BinaryInputNode {
 public:
  static ProduceAllPaths* make(
      QueryContext* qctx, PlanNode* left, PlanNode* right, size_t steps, bool noLoop) {
    return qctx->objPool()->makeAndAdd<ProduceAllPaths>(qctx, left, right, steps, noLoop);
  }

  size_t steps() const {
    return steps_;
  }

  bool noLoop() const {
    return noLoop_;
  }

  std::string leftVidVar() const {
    return leftVidVar_;
  }

  std::string rightVidVar() const {
    return rightVidVar_;
  }

  void setLeftVidVar(const std::string& var) {
    leftVidVar_ = var;
  }

  void setRightVidVar(const std::string& var) {
    rightVidVar_ = var;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  ProduceAllPaths(QueryContext* qctx, PlanNode* left, PlanNode* right, size_t steps, bool noLoop)
      : BinaryInputNode(qctx, Kind::kProduceAllPaths, left, right),
        steps_(steps),
        noLoop_(noLoop) {}

 private:
  size_t steps_{0};
  bool noLoop_{false};
  std::string leftVidVar_;
  std::string rightVidVar_;
};

using VertexProp = nebula::storage::cpp2::VertexProp;
using EdgeProp = nebula::storage::cpp2::EdgeProp;
using Direction = nebula::storage::cpp2::EdgeDirection;

class ShortestPath final : public SingleInputNode {
 public:
  static ShortestPath* make(QueryContext* qctx,
                            PlanNode* node,
                            GraphSpaceID space,
                            bool singleShortest) {
    return qctx->objPool()->makeAndAdd<ShortestPath>(qctx, node, space, singleShortest);
  }

  PlanNode* clone() const override;

  std::unique_ptr<PlanNodeDescription> explain() const override;

  MatchStepRange stepRange() const {
    return range_;
  }

  storage::cpp2::EdgeDirection edgeDirection() const {
    return edgeDirection_;
  }

  const std::vector<EdgeProp>* edgeProps() const {
    return edgeProps_.get();
  }

  const std::vector<EdgeProp>* reverseEdgeProps() const {
    return reverseEdgeProps_.get();
  }

  const std::vector<VertexProp>* vertexProps() const {
    return vertexProps_.get();
  }

  GraphSpaceID space() const {
    return space_;
  }

  bool singleShortest() const {
    return singleShortest_;
  }

  void setStepRange(const MatchStepRange& range) {
    range_ = range;
  }

  void setEdgeDirection(Direction direction) {
    edgeDirection_ = direction;
  }

  void setVertexProps(std::unique_ptr<std::vector<VertexProp>> vertexProps) {
    vertexProps_ = std::move(vertexProps);
  }

  void setEdgeProps(std::unique_ptr<std::vector<EdgeProp>> edgeProps) {
    edgeProps_ = std::move(edgeProps);
  }

  void setReverseEdgeProps(std::unique_ptr<std::vector<EdgeProp>> reverseEdgeProps) {
    reverseEdgeProps_ = std::move(reverseEdgeProps);
  }

 private:
  friend ObjectPool;
  ShortestPath(QueryContext* qctx, PlanNode* node, GraphSpaceID space, bool singleShortest)
      : SingleInputNode(qctx, Kind::kShortestPath, node),
        space_(space),
        singleShortest_(singleShortest) {}

  void cloneMembers(const ShortestPath&);

 private:
  GraphSpaceID space_;
  bool singleShortest_{false};
  MatchStepRange range_;
  std::unique_ptr<std::vector<EdgeProp>> edgeProps_;
  std::unique_ptr<std::vector<EdgeProp>> reverseEdgeProps_;
  std::unique_ptr<std::vector<VertexProp>> vertexProps_;
  storage::cpp2::EdgeDirection edgeDirection_{Direction::OUT_EDGE};
};

class CartesianProduct final : public SingleDependencyNode {
 public:
  static CartesianProduct* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<CartesianProduct>(qctx, input);
  }

  Status addVar(std::string varName);

  std::vector<std::string> inputVars() const;

  std::vector<std::vector<std::string>> allColNames() const {
    return allColNames_;
  }
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  CartesianProduct(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kCartesianProduct, input) {}

  std::vector<std::vector<std::string>> allColNames_;
};

class Subgraph final : public SingleInputNode {
 public:
  static Subgraph* make(QueryContext* qctx,
                        PlanNode* input,
                        GraphSpaceID space,
                        Expression* src,
                        const Expression* tagFilter,
                        const Expression* edgeFilter,
                        const Expression* filter,
                        size_t steps) {
    return qctx->objPool()->makeAndAdd<Subgraph>(
        qctx, input, space, DCHECK_NOTNULL(src), tagFilter, edgeFilter, filter, steps);
  }

  GraphSpaceID space() const {
    return space_;
  }

  Expression* src() const {
    return src_;
  }

  const Expression* tagFilter() const {
    return tagFilter_;
  }

  const Expression* edgeFilter() const {
    return edgeFilter_;
  }

  const Expression* filter() const {
    return filter_;
  }

  size_t steps() const {
    return steps_;
  }

  bool oneMoreStep() const {
    return oneMoreStep_;
  }

  const std::unordered_set<EdgeType> biDirectEdgeTypes() const {
    return biDirectEdgeTypes_;
  }

  const std::vector<EdgeProp>* edgeProps() const {
    return edgeProps_.get();
  }

  const std::vector<VertexProp>* vertexProps() const {
    return vertexProps_.get();
  }

  void setOneMoreStep() {
    oneMoreStep_ = true;
  }

  void setBiDirectEdgeTypes(std::unordered_set<EdgeType> edgeTypes) {
    biDirectEdgeTypes_ = std::move(edgeTypes);
  }

  void setVertexProps(std::unique_ptr<std::vector<VertexProp>> vertexProps) {
    vertexProps_ = std::move(vertexProps);
  }

  void setEdgeProps(std::unique_ptr<std::vector<EdgeProp>> edgeProps) {
    edgeProps_ = std::move(edgeProps);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  Subgraph(QueryContext* qctx,
           PlanNode* input,
           GraphSpaceID space,
           Expression* src,
           const Expression* tagFilter,
           const Expression* edgeFilter,
           const Expression* filter,
           size_t steps)
      : SingleInputNode(qctx, Kind::kSubgraph, input),
        space_(space),
        src_(src),
        tagFilter_(tagFilter),
        edgeFilter_(edgeFilter),
        filter_(filter),
        steps_(steps) {}

  GraphSpaceID space_;
  // vertices may be parsing from runtime.
  Expression* src_{nullptr};
  const Expression* tagFilter_{nullptr};
  const Expression* edgeFilter_{nullptr};
  const Expression* filter_{nullptr};
  size_t steps_{1};
  bool oneMoreStep_{false};
  std::unordered_set<EdgeType> biDirectEdgeTypes_;
  std::unique_ptr<std::vector<VertexProp>> vertexProps_;
  std::unique_ptr<std::vector<EdgeProp>> edgeProps_;
};

class CrossJoin final : public BinaryInputNode {
 public:
  static CrossJoin* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
    return qctx->objPool()->makeAndAdd<CrossJoin>(qctx, left, right);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  PlanNode* clone() const override;

  void accept(PlanNodeVisitor* visitor) override;

 private:
  friend ObjectPool;

  // used for clone only
  static CrossJoin* make(QueryContext* qctx) {
    return qctx->objPool()->makeAndAdd<CrossJoin>(qctx);
  }

  void cloneMembers(const CrossJoin& r);

  CrossJoin(QueryContext* qctx, PlanNode* left, PlanNode* right);
  // use for clone
  explicit CrossJoin(QueryContext* qctx);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLAN_ALGO_H_
