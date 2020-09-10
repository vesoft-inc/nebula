/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_OPTRULE_H_
#define OPTIMIZER_OPTRULE_H_

#include <memory>
#include <string>
#include <vector>

#include "common/base/Status.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {
class QueryContext;
}   // namespace graph

namespace opt {

class OptGroupExpr;

class OptRule {
public:
    struct TransformResult {
        bool eraseCurr;
        bool eraseAll;
        std::vector<OptGroupExpr *> newGroupExprs;
    };

    virtual ~OptRule() = default;

    virtual bool match(const OptGroupExpr *groupExpr) const = 0;
    virtual Status transform(graph::QueryContext *qctx,
                             const OptGroupExpr *groupExpr,
                             TransformResult *result) const = 0;
    virtual std::string toString() const = 0;

protected:
    OptRule() = default;
};

class RuleSet final {
public:
    static RuleSet &defaultRules();
    static RuleSet &queryRules();

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

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTRULE_H_
