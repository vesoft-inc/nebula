/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include "context/QueryContext.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/MultiOutputsExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

Scheduler::Task::Task(const Executor *e) : planId(DCHECK_NOTNULL(e)->node()->id()) {}

Scheduler::Scheduler(QueryContext *qctx) : qctx_(DCHECK_NOTNULL(qctx)) {}

folly::Future<Status> Scheduler::schedule() {
    auto executor = Executor::makeExecutor(qctx_->plan()->root(), qctx_);
    analyze(executor);
    return schedule(executor);
}

void Scheduler::analyze(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kMultiOutputs: {
            const auto &name = executor->node()->varName();
            auto it = multiOutputPromiseMap_.find(name);
            if (it == multiOutputPromiseMap_.end()) {
                MultiOutputsData data(executor->successors().size());
                multiOutputPromiseMap_.emplace(name, std::move(data));
            }
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<SelectExecutor *>(executor);
            analyze(sel->thenBody());
            analyze(sel->elseBody());
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            analyze(loop->loopBody());
            break;
        }
        default:
            break;
    }

    for (auto dep : executor->depends()) {
        analyze(dep);
    }
}

folly::Future<Status> Scheduler::schedule(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<SelectExecutor *>(executor);
            return schedule(sel->depends())
                .then(task(sel,
                           [sel](Status status) {
                               if (!status.ok()) return sel->error(std::move(status));
                               sel->startProfiling();
                               return sel->execute().ensure([sel]() { sel->stopProfiling(); });
                           }))
                .then(task(sel, [sel, this](Status status) {
                    if (!status.ok()) return sel->error(std::move(status));

                    auto val = qctx_->ectx()->getValue(sel->node()->varName());
                    auto cond = val.moveBool();
                    return schedule(cond ? sel->thenBody() : sel->elseBody());
                }));
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            return schedule(loop->depends()).then(task(loop, [loop, this](Status status) {
                if (!status.ok()) return loop->error(std::move(status));
                return iterate(loop);
            }));
        }
        case PlanNode::Kind::kMultiOutputs: {
            auto mout = static_cast<MultiOutputsExecutor *>(executor);
            auto it = multiOutputPromiseMap_.find(mout->node()->varName());
            CHECK(it != multiOutputPromiseMap_.end());

            auto &data = it->second;

            folly::SpinLockGuard g(data.lock);
            if (data.numOutputs == 0) {
                // Reset promise of output executors when it's in loop
                data.numOutputs = static_cast<int32_t>(mout->successors().size());
                data.promise = std::make_unique<folly::SharedPromise<Status>>();
            }

            data.numOutputs--;
            if (data.numOutputs > 0) {
                return data.promise->getFuture();
            }

            return schedule(mout->depends()).then(task(mout, [&data, mout](Status status) {
                // Notify and wake up all waited tasks
                data.promise->setValue(status);

                if (!status.ok()) return mout->error(std::move(status));

                mout->startProfiling();
                return mout->execute().ensure([mout]() { mout->stopProfiling(); });
            }));
        }
        default: {
            auto deps = executor->depends();
            if (deps.empty()) {
                executor->startProfiling();
                return executor->execute().ensure([executor]() { executor->stopProfiling(); });
            }

            return schedule(deps).then(task(executor, [executor](Status stats) {
                if (!stats.ok()) return executor->error(std::move(stats));
                executor->startProfiling();
                return executor->execute().ensure([executor]() { executor->stopProfiling(); });
            }));
        }
    }
}

folly::Future<Status> Scheduler::schedule(const std::set<Executor *> &dependents) {
    CHECK(!dependents.empty());

    std::vector<folly::Future<Status>> futures;
    for (auto dep : dependents) {
        futures.emplace_back(schedule(dep));
    }
    return folly::collect(futures).then([](std::vector<Status> stats) {
        for (auto &s : stats) {
            if (!s.ok()) return s;
        }
        return Status::OK();
    });
}

folly::Future<Status> Scheduler::iterate(LoopExecutor *loop) {
    loop->startProfiling();
    return loop->execute()
        .ensure([loop]() { loop->stopProfiling(); })
        .then(task(loop, [loop, this](Status status) {
            if (!status.ok()) return loop->error(std::move(status));

            auto val = qctx_->ectx()->getValue(loop->node()->varName());
            if (!val.isBool()) {
                std::stringstream ss;
                ss << "Loop produces a bad condition result: " << val << " type: " << val.type();
                return loop->error(Status::Error(ss.str()));
            }
            auto cond = val.moveBool();
            if (!cond) return folly::makeFuture(Status::OK());
            return schedule(loop->loopBody()).then(task(loop, [loop, this](Status s) {
                if (!s.ok()) return loop->error(std::move(s));
                return iterate(loop);
            }));
        }));
}

}   // namespace graph
}   // namespace nebula
