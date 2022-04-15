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
  if (pos.empty()) {
    return DCHECK_NOTNULL(node)->node();
  }

  DCHECK_EQ(pos[0], 0);

  const MatchedResult *result = this;
  for (size_t i = 1; i < pos.size(); ++i) {
    DCHECK_LT(pos[i], result->dependencies.size());
    result = &result->dependencies[pos[i]];
  }
  return DCHECK_NOTNULL(result->node)->node();
}

void MatchedResult::collectBoundary(std::vector<OptGroup *> &boundary) const {
  if (dependencies.empty()) {
    boundary.insert(boundary.end(), node->dependencies().begin(), node->dependencies().end());
  } else {
    for (const auto &dep : dependencies) {
      dep.collectBoundary(boundary);
    }
  }
}

Pattern Pattern::create(graph::PlanNode::Kind kind, std::initializer_list<Pattern> patterns) {
  return Pattern(kind, std::move(patterns));
}

/*static*/ Pattern Pattern::create(std::initializer_list<graph::PlanNode::Kind> kinds,
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

/*static*/ bool OptRule::TransformResult::checkDataFlow(const OptGroupNode *groupNode,
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
    for (std::size_t i = 0; i < deps.size(); i++) {
      const OptGroup *dep = deps[i];
      if (node->inputVar(i) != dep->outputVar()) {
        return false;
      }
      // Only use by father plan node
      if (node->inputVars()[i]->readBy.size() != 1) {
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
      if (!optGNode) continue;
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

void RuleSet::merge(const RuleSet &ruleset) {
  for (auto rule : ruleset.rules()) {
    addRule(rule);
  }
}

}  // namespace opt
}  // namespace nebula
