/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/Optimizer.h"

#include "context/QueryContext.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptRule.h"
#include "planner/plan/ExecutionPlan.h"
#include "planner/plan/Logic.h"
#include "planner/plan/PlanNode.h"

using nebula::graph::BinaryInputNode;
using nebula::graph::Loop;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Select;
using nebula::graph::SingleDependencyNode;

namespace nebula {
namespace opt {

Optimizer::Optimizer(std::vector<const RuleSet *> ruleSets) : ruleSets_(std::move(ruleSets)) {}

StatusOr<const PlanNode *> Optimizer::findBestPlan(QueryContext *qctx) {
    DCHECK(qctx != nullptr);
    auto optCtx = std::make_unique<OptContext>(qctx);

    auto root = qctx->plan()->root();
    auto status = prepare(optCtx.get(), root);
    NG_RETURN_IF_ERROR(status);
    auto rootGroup = std::move(status).value();

    NG_RETURN_IF_ERROR(doExploration(optCtx.get(), rootGroup));
    return rootGroup->getPlan();
}

StatusOr<OptGroup *> Optimizer::prepare(OptContext *ctx, PlanNode *root) {
    std::unordered_map<int64_t, OptGroup *> visited;
    return convertToGroup(ctx, root, &visited);
}

Status Optimizer::doExploration(OptContext *octx, OptGroup *rootGroup) {
    int8_t appliedTimes = kMaxIterationRound;
    while (octx->changed()) {
        if (--appliedTimes < 0) break;
        octx->setChanged(false);
        for (auto ruleSet : ruleSets_) {
            for (auto rule : ruleSet->rules()) {
                NG_RETURN_IF_ERROR(rootGroup->exploreUntilMaxRound(rule));
                rootGroup->setUnexplored(rule);
            }
        }
    }
    return Status::OK();
}

OptGroup *Optimizer::convertToGroup(OptContext *ctx,
                                    PlanNode *node,
                                    std::unordered_map<int64_t, OptGroup *> *visited) {
    auto iter = visited->find(node->id());
    if (iter != visited->cend()) {
        return iter->second;
    }

    auto group = OptGroup::create(ctx);
    auto groupNode = group->makeGroupNode(node);

    if (node->kind() == PlanNode::Kind::kSelect) {
        auto select = static_cast<Select *>(node);
        addBodyToGroupNode(ctx, select->then(), groupNode, visited);
        addBodyToGroupNode(ctx, select->otherwise(), groupNode, visited);
    } else if (node->kind() == PlanNode::Kind::kLoop) {
        auto loop = static_cast<Loop *>(node);
        addBodyToGroupNode(ctx, loop->body(), groupNode, visited);
    }

    for (size_t i = 0; i < node->numDeps(); ++i) {
        auto dep = const_cast<PlanNode *>(node->dep(i));
        groupNode->dependsOn(convertToGroup(ctx, dep, visited));
    }

    visited->emplace(node->id(), group);
    return group;
}

void Optimizer::addBodyToGroupNode(OptContext *ctx,
                                   const PlanNode *node,
                                   OptGroupNode *gnode,
                                   std::unordered_map<int64_t, OptGroup *> *visited) {
    auto n = const_cast<PlanNode *>(node);
    auto body = convertToGroup(ctx, n, visited);
    gnode->addBody(body);
}

}   // namespace opt
}   // namespace nebula
