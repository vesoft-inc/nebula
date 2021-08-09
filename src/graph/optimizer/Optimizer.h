/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_OPTIMIZER_H_
#define OPTIMIZER_OPTIMIZER_H_

#include <memory>

#include "common/base/StatusOr.h"
#include "common/base/Base.h"

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;
}   // namespace graph

namespace opt {

class OptContext;
class OptGroup;
class OptGroupNode;
class RuleSet;

class Optimizer final {
public:
    explicit Optimizer(std::vector<const RuleSet *> ruleSets);
    ~Optimizer() = default;

    StatusOr<const graph::PlanNode *> findBestPlan(graph::QueryContext *qctx);

private:
    StatusOr<OptGroup *> prepare(OptContext *ctx, graph::PlanNode *root);
    Status doExploration(OptContext *octx, OptGroup *rootGroup);

    OptGroup *convertToGroup(OptContext *ctx,
                             graph::PlanNode *node,
                             std::unordered_map<int64_t, OptGroup *> *visited);
    void addBodyToGroupNode(OptContext *ctx,
                            const graph::PlanNode *node,
                            OptGroupNode *gnode,
                            std::unordered_map<int64_t, OptGroup *> *visited);

    static constexpr int8_t kMaxIterationRound = 5;

    std::vector<const RuleSet *> ruleSets_;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTIMIZER_H_
