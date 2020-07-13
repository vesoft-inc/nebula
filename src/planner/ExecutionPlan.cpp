/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"
#include "exec/Executor.h"
#include "util/IdGenerator.h"
#include "planner/PlanNode.h"
#include "scheduler/Scheduler.h"
#include "util/ObjectPool.h"

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(ObjectPool* objectPool)
    : id_(EPIdGenerator::instance().id()),
      objPool_(objectPool),
      nodeIdGen_(std::make_unique<IdGenerator>(0)) {}

ExecutionPlan::~ExecutionPlan() {
}

PlanNode* ExecutionPlan::addPlanNode(PlanNode* node) {
    node->setId(nodeIdGen_->id());
    return objPool_->add(node);
}
}   // namespace graph
}   // namespace nebula
