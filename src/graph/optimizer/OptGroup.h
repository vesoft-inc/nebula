/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_OPTGROUP_H_
#define GRAPH_OPTIMIZER_OPTGROUP_H_

#include "common/base/ObjectPool.h"
#include "common/base/Status.h"

namespace nebula {
namespace graph {
class PlanNode;
}  // namespace graph

namespace opt {

class OptContext;
class OptGroupNode;
class OptRule;

class OptGroup final {
 public:
  static OptGroup *create(OptContext *ctx);

  bool isExplored(const OptRule *rule) const {
    return std::find(exploredRules_.cbegin(), exploredRules_.cend(), rule) != exploredRules_.cend();
  }

  void setExplored(const OptRule *rule) {
    exploredRules_.emplace_back(rule);
  }

  void setUnexplored(const OptRule *rule);

  void addGroupNode(OptGroupNode *groupNode);
  OptGroupNode *makeGroupNode(graph::PlanNode *node);
  const std::list<OptGroupNode *> &groupNodes() const {
    return groupNodes_;
  }

  Status explore(const OptRule *rule);
  Status exploreUntilMaxRound(const OptRule *rule);
  double getCost() const;
  const graph::PlanNode *getPlan() const;
  const std::string &outputVar() const {
    return outputVar_;
  }

  void addRefGroupNode(const OptGroupNode *node) {
    groupNodesReferenced_.insert(node);
  }

  void deleteRefGroupNode(const OptGroupNode *node);

  void setRootGroup() {
    isRootGroup_ = true;
  }

  Status validate(const OptRule *rule) const;

 private:
  friend ObjectPool;
  explicit OptGroup(OptContext *ctx) noexcept;

  static constexpr int16_t kMaxExplorationRound = 128;

  std::pair<double, const OptGroupNode *> findMinCostGroupNode() const;
  Status validateSubPlan(const OptGroupNode *gn,
                         const OptRule *rule,
                         const std::vector<OptGroup *> &patternLeaves) const;

  OptContext *ctx_{nullptr};
  std::list<OptGroupNode *> groupNodes_;
  std::vector<const OptRule *> exploredRules_;
  // The output variable should be same across the whole group.
  std::string outputVar_;

  bool isRootGroup_{false};
  // Save the OptGroupNode which references this OptGroup
  std::unordered_set<const OptGroupNode *> groupNodesReferenced_;
};

class OptGroupNode final {
 public:
  static OptGroupNode *create(OptContext *ctx, graph::PlanNode *node, const OptGroup *group);

  void dependsOn(OptGroup *dep) {
    dependencies_.emplace_back(dep);
    dep->addRefGroupNode(this);
  }

  const std::vector<OptGroup *> &dependencies() const {
    return dependencies_;
  }

  void setDeps(std::vector<OptGroup *> deps) {
    dependencies_ = deps;
    for (auto *dep : deps) {
      dep->addRefGroupNode(this);
    }
  }

  void addBody(OptGroup *body) {
    bodies_.emplace_back(body);
  }

  const std::vector<OptGroup *> &bodies() const {
    return bodies_;
  }

  bool isExplored(const OptRule *rule) const {
    return std::find(exploredRules_.cbegin(), exploredRules_.cend(), rule) != exploredRules_.cend();
  }

  void setExplored(const OptRule *rule) {
    exploredRules_.emplace_back(rule);
  }

  void setUnexplored(const OptRule *rule);

  const OptGroup *group() const {
    return group_;
  }

  graph::PlanNode *node() const {
    return node_;
  }

  Status explore(const OptRule *rule);
  double getCost() const;
  const graph::PlanNode *getPlan() const;

  // Release the opt group node from its opt group
  void release();

  Status validate(const OptRule *rule) const;

 private:
  friend ObjectPool;
  OptGroupNode(graph::PlanNode *node, const OptGroup *group) noexcept;

  graph::PlanNode *node_{nullptr};
  const OptGroup *group_{nullptr};
  std::vector<OptGroup *> dependencies_;
  std::vector<OptGroup *> bodies_;
  std::vector<const OptRule *> exploredRules_;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_OPTGROUP_H_
