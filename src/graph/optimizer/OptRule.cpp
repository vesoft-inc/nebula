/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/OptRule.h"

#include "common/base/Logging.h"
#include "graph/context/Symbols.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"

using nebula::graph::PlanNode;

namespace nebula {
namespace opt {

const PlanNode *MatchedResult::planNode(const std::vector<int32_t> &pos) const {
  auto *pnode = DCHECK_NOTNULL(result(pos).node)->node();
  return DCHECK_NOTNULL(pnode);
}

const MatchedResult &MatchedResult::result(const std::vector<int32_t> &pos) const {
  if (pos.empty()) {
    return *this;
  }

  DCHECK_EQ(pos[0], 0);

  const MatchedResult *result = this;
  for (size_t i = 1; i < pos.size(); ++i) {
    DCHECK_LT(pos[i], result->dependencies.size());
    result = &result->dependencies[pos[i]];
  }
  return *DCHECK_NOTNULL(result);
}

void MatchedResult::collectPatternLeaves(std::vector<OptGroup *> &leaves) const {
  if (dependencies.empty()) {
    if (node->dependencies().empty()) {
      // nullptr means this node in matched pattern is a leaf node
      leaves.push_back(nullptr);
    } else {
      leaves.insert(leaves.end(), node->dependencies().begin(), node->dependencies().end());
    }
  } else {
    for (const auto &dep : dependencies) {
      dep.collectPatternLeaves(leaves);
    }
  }
}

Pattern Pattern::create(graph::PlanNode::Kind kind, std::initializer_list<Pattern> patterns) {
  return Pattern(kind, std::move(patterns));
}

/*static*/
Pattern Pattern::create(std::initializer_list<graph::PlanNode::Kind> kinds,
                        std::initializer_list<Pattern> patterns) {
  return Pattern(std::move(kinds), std::move(patterns));
}

StatusOr<MatchedResult> Pattern::match(const OptGroupNode *groupNode) const {
  if (!node_.match(groupNode->node())) {
    return Status::Error();
  }

  if (dependencies_.empty()) {
    return MatchedResult{groupNode, {}};
  }

  if (groupNode->dependencies().size() != dependencies_.size()) {
    return Status::Error();
  }

  MatchedResult result;
  result.node = groupNode;
  result.dependencies.reserve(dependencies_.size());
  for (size_t i = 0; i < dependencies_.size(); ++i) {
    auto group = groupNode->dependencies()[i];
    const auto &pattern = dependencies_[i];
    auto status = pattern.match(group);
    NG_RETURN_IF_ERROR(status);
    result.dependencies.emplace_back(std::move(status).value());
  }
  return result;
}

StatusOr<MatchedResult> Pattern::match(const OptGroup *group) const {
  for (auto node : group->groupNodes()) {
    auto status = match(node);
    if (status.ok()) {
      return status;
    }
  }
  return Status::Error();
}

bool OptRule::TransformResult::checkDataFlow(const std::vector<OptGroup *> &boundary) {
  return std::all_of(
      newGroupNodes.begin(), newGroupNodes.end(), [&boundary](const OptGroupNode *groupNode) {
        return checkDataFlow(groupNode, boundary);
      });
}

/*static*/
bool OptRule::TransformResult::checkDataFlow(const OptGroupNode *groupNode,
                                             const std::vector<OptGroup *> &boundary) {
  const auto &deps = groupNode->dependencies();
  // reach the boundary
  if (std::all_of(deps.begin(), deps.end(), [&boundary](OptGroup *dep) {
        return std::find(boundary.begin(), boundary.end(), dep) != boundary.end();
      })) {
    return true;
  }
  const auto *group = groupNode->group();
  if (std::find(boundary.begin(), boundary.end(), group) != boundary.end()) {
    return true;
  }
  // Check dataflow
  const auto *node = groupNode->node();
  if (node->inputVars().size() == deps.size()) {
    // Don't check when count of dependencies is different from count of input variables
    auto checkNumReadBy = [](const graph::Variable *v) -> bool {
      switch (v->readBy.size()) {
        case 1:
          return true;
        case 2:
          // There is at least one Argument plan node if this variable read by 2 other nodes
          return std::any_of(v->readBy.begin(), v->readBy.end(), [](auto *n) {
            return n->kind() == graph::PlanNode::Kind::kArgument;
          });
        default:
          return false;
      }
    };
    for (std::size_t i = 0; i < deps.size(); i++) {
      const OptGroup *dep = deps[i];
      if (node->inputVar(i) != dep->outputVar()) {
        return false;
      }
      // Only use by father plan node
      if (!checkNumReadBy(node->inputVars()[i])) {
        return false;
      }
      return std::all_of(
          dep->groupNodes().begin(), dep->groupNodes().end(), [&boundary](const OptGroupNode *gn) {
            return checkDataFlow(gn, boundary);
          });
    }
  }
  return true;
}

StatusOr<MatchedResult> OptRule::match(OptContext *ctx, const OptGroupNode *groupNode) const {
  const auto &pattern = this->pattern();
  auto status = pattern.match(groupNode);
  NG_RETURN_IF_ERROR(status);
  auto matched = std::move(status).value();
  if (!this->match(ctx, matched)) {
    return Status::Error();
  }
  return matched;
}

bool OptRule::match(OptContext *ctx, const MatchedResult &matched) const {
  return checkDataflowDeps(ctx, matched, matched.node->node()->outputVar(), true);
}

bool OptRule::checkDataflowDeps(OptContext *ctx,
                                const MatchedResult &matched,
                                const std::string &var,
                                bool isRoot) const {
  auto node = matched.node;
  auto planNode = node->node();
  const auto &outVarName = planNode->outputVar();
  if (outVarName != var) {
    return false;
  }
  auto symTbl = ctx->qctx()->symTable();
  auto outVar = symTbl->getVar(outVarName);
  // Check whether the data flow is same as the control flow in execution plan.
  if (!isRoot) {
    for (auto pnode : outVar->readBy) {
      auto optGNode = ctx->findOptGroupNodeByPlanNodeId(pnode->id());
      // Ignore the data dependency introduced by Argument plan node
      if (!optGNode || optGNode->node()->kind() == graph::PlanNode::Kind::kArgument) continue;
      const auto &deps = optGNode->dependencies();
      auto found = std::find(deps.begin(), deps.end(), node->group());
      if (found == deps.end()) {
        VLOG(2) << ctx->qctx()->symTable()->toString();
        return false;
      }
    }
  }

  const auto &deps = matched.dependencies;
  if (deps.empty()) {
    return true;
  }
  DCHECK_EQ(deps.size(), node->dependencies().size());
  for (size_t i = 0; i < deps.size(); ++i) {
    if (!checkDataflowDeps(ctx, deps[i], planNode->inputVar(i), false)) {
      return false;
    }
  }
  return true;
}

RuleSet &RuleSet::DefaultRules() {
  static RuleSet kDefaultRules("DefaultRuleSet");
  return kDefaultRules;
}

RuleSet &RuleSet::QueryRules0() {
  static RuleSet kQueryRules0("QueryRuleSet0");
  return kQueryRules0;
}

RuleSet &RuleSet::QueryRules() {
  static RuleSet kQueryRules("QueryRuleSet");
  return kQueryRules;
}

RuleSet::RuleSet(const std::string &name) : name_(name) {}

RuleSet *RuleSet::addRule(const OptRule *rule) {
  DCHECK(rule != nullptr);
  auto found = std::find(rules_.begin(), rules_.end(), rule);
  if (found == rules_.end()) {
    rules_.emplace_back(rule);
  } else {
    LOG(WARNING) << "Rule set " << name_ << " has contained this rule: " << rule->toString();
  }
  return this;
}

std::string RuleSet::toString() const {
  std::stringstream ss;
  ss << "RuleSet: " << name_ << std::endl;
  for (auto rule : rules_) {
    ss << rule->toString() << std::endl;
  }
  return ss.str();
}

void RuleSet::merge(const RuleSet &ruleset) {
  for (auto rule : ruleset.rules()) {
    addRule(rule);
  }
}

}  // namespace opt
}  // namespace nebula
