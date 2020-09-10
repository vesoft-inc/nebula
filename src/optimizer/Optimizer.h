/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_OPTIMIZER_H_
#define OPTIMIZER_OPTIMIZER_H_

#include <memory>

#include "common/base/StatusOr.h"

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;
}   // namespace graph

namespace opt {

class OptGroup;
class OptGroupExpr;
class RuleSet;

class Optimizer final {
public:
    Optimizer(graph::QueryContext *qctx, std::vector<const RuleSet *> ruleSets);
    ~Optimizer() = default;

    StatusOr<const graph::PlanNode *> findBestPlan(graph::PlanNode *root);

private:
    Status prepare();
    Status doExploration();

    OptGroup *convertToGroup(graph::PlanNode *node);

    graph::QueryContext *qctx_{nullptr};
    graph::PlanNode *rootNode_{nullptr};
    OptGroup *rootGroup_{nullptr};
    std::vector<const RuleSet *> ruleSets_;
    std::unordered_map<int64_t, OptGroup *> visitedNodes_;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTIMIZER_H_
