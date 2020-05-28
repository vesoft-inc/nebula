/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_EXECUTIONPLAN_H_
#define PLANNER_EXECUTIONPLAN_H_

#include <cstdint>
#include <memory>

#include <folly/futures/Future.h>

#include "common/base/Status.h"

namespace nebula {

class ObjectPool;

namespace graph {

class ExecutionContext;
class Executor;
class IdGenerator;
class PlanNode;
class ExecutionContext;
class Scheduler;

class ExecutionPlan final {
public:
    explicit ExecutionPlan(ExecutionContext* ectx);

    ~ExecutionPlan();

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    PlanNode* addPlanNode(PlanNode* node);

    int64_t id() const {
        return id_;
    }

    const PlanNode* root() const {
        return root_;
    }

    folly::Future<Status> execute();

private:
    Executor* createExecutor();

    int64_t                                 id_;
    PlanNode*                               root_{nullptr};
    ExecutionContext*                       ectx_{nullptr};
    std::unique_ptr<IdGenerator>            nodeIdGen_;
    std::unique_ptr<Scheduler>              scheduler_;
};

}  // namespace graph
}  // namespace nebula

#endif
