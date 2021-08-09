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
}   // namespace graph

namespace opt {

class OptContext;
class OptGroupNode;
class OptRule;

class OptGroup final {
public:
    static OptGroup *create(OptContext *ctx);

    bool isExplored(const OptRule *rule) const {
        return std::find(exploredRules_.cbegin(), exploredRules_.cend(), rule) !=
               exploredRules_.cend();
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

private:
    explicit OptGroup(OptContext *ctx) noexcept;

    static constexpr int16_t kMaxExplorationRound = 128;

    std::pair<double, const OptGroupNode *> findMinCostGroupNode() const;

    OptContext *ctx_{nullptr};
    std::list<OptGroupNode *> groupNodes_;
    std::vector<const OptRule *> exploredRules_;
};

class OptGroupNode final {
public:
    static OptGroupNode *create(OptContext *ctx, graph::PlanNode *node, const OptGroup *group);

    void dependsOn(OptGroup *dep) {
        dependencies_.emplace_back(dep);
    }

    const std::vector<OptGroup *> &dependencies() const {
        return dependencies_;
    }

    void setDeps(std::vector<OptGroup *> deps) {
        dependencies_ = deps;
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

private:
    OptGroupNode(graph::PlanNode *node, const OptGroup *group) noexcept;

    graph::PlanNode *node_{nullptr};
    const OptGroup *group_{nullptr};
    std::vector<OptGroup *> dependencies_;
    std::vector<OptGroup *> bodies_;
    std::vector<const OptRule *> exploredRules_;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTGROUP_H_
