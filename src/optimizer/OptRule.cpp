/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptRule.h"

#include "common/base/Logging.h"
#include "optimizer/OptGroup.h"

namespace nebula {
namespace opt {

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

StatusOr<MatchedResult> OptRule::match(const OptGroupNode *groupNode) const {
    const auto &pattern = this->pattern();
    auto status = pattern.match(groupNode);
    NG_RETURN_IF_ERROR(status);
    auto matched = std::move(status).value();
    if (!this->match(matched)) {
        return Status::Error();
    }
    return matched;
}

bool OptRule::match(const MatchedResult &matched) const {
    UNUSED(matched);
    // Return true if subclass doesn't override this interface,
    // so optimizer will only check whether pattern is matched
    return true;
}

RuleSet &RuleSet::DefaultRules() {
    static RuleSet kDefaultRules("DefaultRuleSet");
    return kDefaultRules;
}

RuleSet &RuleSet::QueryRules() {
    static RuleSet kQueryRules("QueryRules");
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
