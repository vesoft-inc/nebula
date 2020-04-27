/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include "exec/Executor.h"
#include "planner/IdGenerator.h"
#include "planner/PlanNode.h"
#include "service/ExecutionContext.h"
#include "util/ObjectPool.h"

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(ExecutionContext* ectx)
    : id_(EPIdGenerator::instance().id()),
      ectx_(ectx),
      nodeIdGen_(std::make_unique<IdGenerator>(0)) {
    DCHECK_NOTNULL(ectx);
}

ExecutionPlan::~ExecutionPlan() {
    ectx_ = nullptr;
}

PlanNode* ExecutionPlan::addPlanNode(PlanNode* node) {
    node->setId(nodeIdGen_->id());
    return ectx_->objPool()->add(node);
}

Executor* ExecutionPlan::createExecutor() {
    std::unordered_map<int64_t, Executor*> cache;
    return Executor::makeExecutor(root_, ectx_, &cache);
}

}   // namespace graph
}   // namespace nebula
