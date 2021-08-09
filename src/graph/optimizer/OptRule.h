/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_OPTRULE_H_
#define OPTIMIZER_OPTRULE_H_

#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "common/base/StatusOr.h"
#include "planner/plan/PlanNode.h"

namespace nebula {

namespace graph {
class QueryContext;
class PlanNode;
}   // namespace graph

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
};

class Pattern final {
public:
    static Pattern create(graph::PlanNode::Kind kind, std::initializer_list<Pattern> patterns = {});

    StatusOr<MatchedResult> match(const OptGroupNode *groupNode) const;

private:
    Pattern() = default;
    StatusOr<MatchedResult> match(const OptGroup *group) const;

    graph::PlanNode::Kind kind_;
    std::vector<Pattern> dependencies_;
};

class OptRule {
public:
    struct TransformResult {
        static const TransformResult &noTransform() {
            static TransformResult kNoTrans{false, false, {}};
            return kNoTrans;
        }

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

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTRULE_H_
