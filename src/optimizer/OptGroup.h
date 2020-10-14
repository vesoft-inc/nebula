/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_OPTGROUP_H_
#define OPTIMIZER_OPTGROUP_H_

#include <algorithm>
#include <list>
#include <vector>
#include "common/base/Status.h"

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;
}   // namespace graph

namespace opt {

class OptGroupExpr;
class OptRule;

class OptGroup final {
public:
    static OptGroup *create(graph::QueryContext *qctx);

    bool isExplored(const OptRule *rule) const {
        return std::find(exploredRules_.cbegin(), exploredRules_.cend(), rule) !=
               exploredRules_.cend();
    }

    void setExplored(const OptRule *rule) {
        exploredRules_.emplace_back(rule);
    }

    void setUnexplored(const OptRule *rule) {
        auto iter = std::find(exploredRules_.begin(), exploredRules_.end(), rule);
        if (iter != exploredRules_.end()) {
            exploredRules_.erase(iter);
        }
    }

    void addGroupExpr(OptGroupExpr *groupExpr);
    OptGroupExpr *makeGroupExpr(graph::QueryContext *qctx, graph::PlanNode *node);
    const std::list<OptGroupExpr *> &groupExprs() const {
        return groupExprs_;
    }

    Status explore(const OptRule *rule);
    Status exploreUtilMaxRound(const OptRule *rule);
    double getCost() const;
    const graph::PlanNode *getPlan() const;

private:
    explicit OptGroup(graph::QueryContext *qctx) noexcept;

    static constexpr int16_t kMaxExplorationRound = 128;

    std::pair<double, const OptGroupExpr *> findMinCostGroupExpr() const;

    graph::QueryContext *qctx_{nullptr};
    std::list<OptGroupExpr *> groupExprs_;
    std::vector<const OptRule *> exploredRules_;
};

class OptGroupExpr final {
public:
    static OptGroupExpr *create(graph::QueryContext *qctx,
                                graph::PlanNode *node,
                                const OptGroup *group);

    void dependsOn(OptGroup *dep) {
        dependencies_.emplace_back(dep);
    }

    const std::vector<OptGroup *> &dependencies() const {
        return dependencies_;
    }

    void addBody(OptGroup *body) {
        bodies_.emplace_back(body);
    }

    const std::vector<OptGroup *> &bodies() const {
        return bodies_;
    }

    bool isExplored(const OptRule *rule) const {
        return std::find(exploredRules_.cbegin(), exploredRules_.cend(), rule) !=
               exploredRules_.cend();
    }

    void setExplored(const OptRule *rule) {
        exploredRules_.emplace_back(rule);
    }

    const OptGroup *group() const {
        return group_;
    }

    const graph::PlanNode *node() const {
        return node_;
    }

    Status explore(const OptRule *rule);
    double getCost() const;
    const graph::PlanNode *getPlan() const;

private:
    OptGroupExpr(graph::PlanNode *node, const OptGroup *group) noexcept;

    graph::PlanNode *node_{nullptr};
    const OptGroup *group_{nullptr};
    std::vector<OptGroup *> dependencies_;
    std::vector<OptGroup *> bodies_;
    std::vector<const OptRule *> exploredRules_;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTGROUP_H_
