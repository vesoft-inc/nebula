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
#include "executor/logic/PassThroughExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

Scheduler::Task::Task(const Executor *e) : planId(DCHECK_NOTNULL(e)->node()->id()) {}

Scheduler::PassThroughData::PassThroughData(int32_t outputs)
    : promise(std::make_unique<folly::SharedPromise<Status>>()), numOutputs(outputs) {}

Scheduler::Scheduler(QueryContext *qctx) : qctx_(DCHECK_NOTNULL(qctx)) {}

folly::Future<Status> Scheduler::schedule() {
    auto executor = Executor::create(qctx_->plan()->root(), qctx_);
    analyze(executor);
    return doSchedule(executor);
}

void Scheduler::analyze(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kPassThrough: {
            // Use id to identify different passThrough nodes to prevent dead lock
            auto id = executor->node()->id();
            auto it = passThroughPromiseMap_.find(id);
            if (it == passThroughPromiseMap_.end()) {
                PassThroughData data(executor->successors().size());
                passThroughPromiseMap_.emplace(id, std::move(data));
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

folly::Future<Status> Scheduler::doSchedule(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            auto sel = static_cast<SelectExecutor *>(executor);
            return doScheduleParallel(sel->depends())
                .thenValue(task(sel,
                           [sel, this](Status status) {
                               if (!status.ok()) return sel->error(std::move(status));
                               return execute(sel);
                           }))
                .thenValue(task(sel, [sel, this](Status status) {
                    if (!status.ok()) return sel->error(std::move(status));

                    auto val = qctx_->ectx()->getValue(sel->node()->outputVar());
                    auto cond = val.moveBool();
                    return doSchedule(cond ? sel->thenBody() : sel->elseBody());
                }));
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            return doScheduleParallel(loop->depends())
                .thenValue(task(loop, [loop, this](Status status) {
                    if (!status.ok()) return loop->error(std::move(status));
                    return iterate(loop);
            }));
        }
        case PlanNode::Kind::kPassThrough: {
            auto mout = static_cast<PassThroughExecutor *>(executor);
            auto it = passThroughPromiseMap_.find(mout->node()->id());
            CHECK(it != passThroughPromiseMap_.end());

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

            return doScheduleParallel(mout->depends())
                .thenValue(task(mout, [&data, mout, this](Status status) {
                    // Notify and wake up all waited tasks
                    data.promise->setValue(status);

                    if (!status.ok()) return mout->error(std::move(status));
                    return execute(mout);
                }));
        }
        default: {
            auto deps = executor->depends();
            if (deps.empty()) {
                return execute(executor);
            }

            return doScheduleParallel(deps)
                .thenValue(task(executor, [executor, this](Status stats) {
                    if (!stats.ok()) return executor->error(std::move(stats));
                    return execute(executor);
            }));
        }
    }
}

folly::Future<Status> Scheduler::doScheduleParallel(const std::set<Executor *> &dependents) {
    CHECK(!dependents.empty());

    std::vector<folly::Future<Status>> futures;
    for (auto dep : dependents) {
        futures.emplace_back(doSchedule(dep));
    }
    auto *runner = qctx_->rctx()->runner();
    return folly::collect(futures).via(runner).thenValue([](std::vector<Status> stats) {
        for (auto &s : stats) {
            if (!s.ok()) return s;
        }
        return Status::OK();
    });
}

folly::Future<Status> Scheduler::iterate(LoopExecutor *loop) {
    return execute(loop).thenValue(task(loop, [loop, this](Status status) {
        if (!status.ok()) return loop->error(std::move(status));

        auto val = qctx_->ectx()->getValue(loop->node()->outputVar());
        if (!val.isBool()) {
            std::stringstream ss;
            ss << "Loop produces a bad condition result: " << val << " type: " << val.type();
            return loop->error(Status::Error(ss.str()));
        }
        auto cond = val.moveBool();
        if (!cond) return folly::makeFuture(Status::OK());
        return doSchedule(loop->loopBody()).thenValue(task(loop, [loop, this](Status s) {
            if (!s.ok()) return loop->error(std::move(s));
            return iterate(loop);
        }));
    }));
}

folly::Future<Status> Scheduler::execute(Executor *executor) {
    auto status = executor->open();
    if (!status.ok()) {
        return executor->error(std::move(status));
    }
    return executor->execute().thenValue([executor](Status s) {
        NG_RETURN_IF_ERROR(s);
        return executor->close();
    });
}

}   // namespace graph
}   // namespace nebula
