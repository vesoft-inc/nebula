/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptRule.h"

#include "common/base/Logging.h"
#include "context/Symbols.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"

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

Pattern Pattern::create(graph::PlanNode::Kind kind, std::initializer_list<Pattern> patterns) {
    Pattern pattern;
    pattern.kind_ = kind;
    for (auto &p : patterns) {
        pattern.dependencies_.emplace_back(p);
    }
    return pattern;
}

StatusOr<MatchedResult> Pattern::match(const OptGroupNode *groupNode) const {
    if (groupNode->node()->kind() != kind_) {
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
            if (deps.empty()) continue;
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

}   // namespace opt
}   // namespace nebula
