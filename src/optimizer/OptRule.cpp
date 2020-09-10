/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptRule.h"

#include "common/base/Logging.h"

namespace nebula {
namespace opt {

RuleSet &RuleSet::defaultRules() {
    static RuleSet kDefaultRules("DefaultRuleSet");
    return kDefaultRules;
}

RuleSet &RuleSet::queryRules() {
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
