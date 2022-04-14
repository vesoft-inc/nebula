/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_OPTRULE_H_
#define GRAPH_OPTIMIZER_OPTRULE_H_

#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "common/base/StatusOr.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {

namespace graph {
class QueryContext;
class PlanNode;
}  // namespace graph

namespace opt {

class OptContext;
class OptGroupNode;
class OptGroup;

struct MatchedResult {
  const OptGroupNode *node{nullptr};
  std::vector<MatchedResult> dependencies;

  // params       | plan node
  // -------------+------------
  // {}           | this->node
  // {0}          | this->node
  // {1}          | error
  // {0, 1}       | this->dependencies[1]
  // {0, 1, 0}    | this->dependencies[1].dependencies[0]
  // {0, 1, 0, 1} | this->dependencies[1].dependencies[0].dependencies[1]
  const graph::PlanNode *planNode(const std::vector<int32_t> &pos = {}) const;

  void collectBoundary(std::vector<OptGroup *> &boundary) const;
};

// Match plan node by trait or kind of plan node.
class MatchNode {
 public:
  explicit MatchNode(graph::PlanNode::Kind kind) : node_({kind}) {}
  explicit MatchNode(std::initializer_list<graph::PlanNode::Kind> kinds)
      : node_(std::move(kinds)) {}

  bool match(const graph::PlanNode *node) const {
    auto find = node_.find(node->kind());
    return find != node_.end();
  }

 private:
  const std::unordered_set<graph::PlanNode::Kind> node_;
};

class Pattern final {
 public:
  static Pattern create(graph::PlanNode::Kind kind, std::initializer_list<Pattern> patterns = {});
  static Pattern create(std::initializer_list<graph::PlanNode::Kind> kinds,
                        std::initializer_list<Pattern> patterns = {});

  StatusOr<MatchedResult> match(const OptGroupNode *groupNode) const;

 private:
  explicit Pattern(graph::PlanNode::Kind kind, std::initializer_list<Pattern> patterns = {})
      : node_(kind), dependencies_(patterns) {}
  explicit Pattern(std::initializer_list<graph::PlanNode::Kind> kinds,
                   std::initializer_list<Pattern> patterns = {})
      : node_(std::move(kinds)), dependencies_(patterns) {}
  StatusOr<MatchedResult> match(const OptGroup *group) const;

  MatchNode node_;
  std::vector<Pattern> dependencies_;
};

class OptRule {
 public:
  struct TransformResult {
    static const TransformResult &noTransform() {
      static TransformResult kNoTrans{false, false, {}};
      return kNoTrans;
    }

    // The plan of result should keep dataflow same as dependencies
    bool checkDataFlow(const std::vector<OptGroup *> &boundary);
    static bool checkDataFlow(const OptGroupNode *groupNode,
                              const std::vector<OptGroup *> &boundary);

    bool eraseCurr{false};
    bool eraseAll{false};
    std::vector<OptGroupNode *> newGroupNodes;
  };

  StatusOr<MatchedResult> match(OptContext *ctx, const OptGroupNode *groupNode) const;

  virtual ~OptRule() = default;

  virtual const Pattern &pattern() const = 0;
  virtual bool match(OptContext *ctx, const MatchedResult &matched) const;
  virtual StatusOr<TransformResult> transform(OptContext *ctx,
                                              const MatchedResult &matched) const = 0;
  virtual std::string toString() const = 0;

 protected:
  OptRule() = default;

  // Return false if the output variable of this matched plan node is not the
  // input of other plan node
  bool checkDataflowDeps(OptContext *ctx,
                         const MatchedResult &matched,
                         const std::string &var,
                         bool isRoot) const;
};

class RuleSet final {
 public:
  static RuleSet &DefaultRules();
  static RuleSet &QueryRules();

  RuleSet *addRule(const OptRule *rule);

  void merge(const RuleSet &ruleset);

  const std::vector<const OptRule *> &rules() const {
    return rules_;
  }

 private:
  explicit RuleSet(const std::string &name);

  std::string name_;
  std::vector<const OptRule *> rules_;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_OPTRULE_H_
