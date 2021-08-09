/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/AsyncMsgNotifyBasedScheduler.h"

DECLARE_bool(enable_lifetime_optimize);

namespace nebula {
namespace graph {
AsyncMsgNotifyBasedScheduler::AsyncMsgNotifyBasedScheduler(QueryContext* qctx) : Scheduler() {
    qctx_ = qctx;
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::schedule() {
    if (FLAGS_enable_lifetime_optimize) {
        // special for root
        qctx_->plan()->root()->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                                               std::memory_order_relaxed);
        analyzeLifetime(qctx_->plan()->root());
    }
    auto executor = Executor::create(qctx_->plan()->root(), qctx_);
    return doSchedule(executor);
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::doSchedule(Executor* root) const {
    std::unordered_map<int64_t, std::vector<folly::Promise<Status>>> promiseMap;
    std::unordered_map<int64_t, std::vector<folly::Future<Status>>> futureMap;
    std::queue<Executor*> queue;
    std::queue<Executor*> queue2;
    std::unordered_set<Executor*> visited;

    auto* runner = qctx_->rctx()->runner();
    folly::Promise<Status> promiseForRoot;
    auto resultFuture = promiseForRoot.getFuture();
    promiseMap[root->id()].emplace_back(std::move(promiseForRoot));
    queue.push(root);
    visited.emplace(root);
    while (!queue.empty()) {
        auto* exe = queue.front();
        queue.pop();
        queue2.push(exe);

        std::vector<folly::Future<Status>> futures;
        for (auto* dep : exe->depends()) {
            auto notVisited = visited.emplace(dep).second;
            if (notVisited) {
                queue.push(dep);
            }
            folly::Promise<Status> p;
            futures.emplace_back(p.getFuture());
            auto& promises = promiseMap[dep->id()];
            promises.emplace_back(std::move(p));
        }
        futureMap.emplace(exe->id(), std::move(futures));
    }

    while (!queue2.empty()) {
        auto* exe = queue2.front();
        queue2.pop();

        auto currentFuturesFound = futureMap.find(exe->id());
        DCHECK(currentFuturesFound != futureMap.end());
        auto currentExeFutures = std::move(currentFuturesFound->second);

        auto currentPromisesFound = promiseMap.find(exe->id());
        DCHECK(currentPromisesFound != promiseMap.end());
        auto currentExePromises = std::move(currentPromisesFound->second);

        scheduleExecutor(std::move(currentExeFutures), exe, runner, std::move(currentExePromises));
    }

    return resultFuture;
}

void AsyncMsgNotifyBasedScheduler::scheduleExecutor(
    std::vector<folly::Future<Status>>&& futures,
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    switch (exe->node()->kind()) {
        case PlanNode::Kind::kSelect: {
            auto select = static_cast<SelectExecutor*>(exe);
            runSelect(std::move(futures), select, runner, std::move(promises));
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor*>(exe);
            runLoop(std::move(futures), loop, runner, std::move(promises));
            break;
        }
        default: {
            if (exe->depends().empty()) {
                runLeafExecutor(exe, runner, std::move(promises));
            } else {
                runExecutor(std::move(futures), exe, runner, std::move(promises));
            }
            break;
        }
    }
}

void AsyncMsgNotifyBasedScheduler::runSelect(std::vector<folly::Future<Status>>&& futures,
                             SelectExecutor* select,
                             folly::Executor* runner,
                             std::vector<folly::Promise<Status>>&& promises) const {
    folly::collect(futures).via(runner).thenTry(
        [select, pros = std::move(promises), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(pros, Status::Error(t.exception().what()));
            }
            auto status = std::move(t).value();
            auto s = checkStatus(std::move(status));
            if (!s.ok()) {
                return notifyError(pros, s);
            }

            std::move(execute(select))
                .thenTry(
                    [select, pros = std::move(pros), this](auto&& selectTry) mutable {
                        if (selectTry.hasException()) {
                            return notifyError(pros, Status::Error(
                                                        selectTry.exception().what()));
                        }
                        auto selectStatus = std::move(selectTry).value();
                        if (!selectStatus.ok()) {
                            return notifyError(pros, selectStatus);
                        }
                        auto val = qctx_->ectx()->getValue(select->node()->outputVar());
                        if (!val.isBool()) {
                            std::stringstream ss;
                            ss << "Loop produces a bad condition result: " << val
                               << " type: " << val.type();
                            return notifyError(pros, Status::Error(ss.str()));
                        }

                        auto selectFuture = folly::makeFuture<Status>(Status::OK());
                        if (val.getBool()) {
                            selectFuture = doSchedule(select->thenBody());
                        } else {
                            selectFuture = doSchedule(select->elseBody());
                        }
                        std::move(selectFuture)
                            .thenTry([pros = std::move(pros), this](auto&& bodyTry) mutable {
                                if (bodyTry.hasException()) {
                                    return notifyError(pros,
                                                       Status::Error(
                                                           bodyTry.exception().what()));
                                }
                                auto bodyStatus = std::move(bodyTry).value();
                                if (!bodyStatus.ok()) {
                                    return notifyError(pros, bodyStatus);
                                } else {
                                    return notifyOK(pros);
                                }
                            });
                    });
        });
}

void AsyncMsgNotifyBasedScheduler::runExecutor(
    std::vector<folly::Future<Status>>&& futures,
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    folly::collect(futures).via(runner).thenTry(
        [exe, pros = std::move(promises), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(pros, Status::Error(t.exception().what()));
            }
            auto status = std::move(t).value();
            auto depStatus = checkStatus(std::move(status));
            if (!depStatus.ok()) {
                return notifyError(pros, depStatus);
            }
            // Execute in current thread.
            std::move(execute(exe)).thenTry(
                [pros = std::move(pros), this](auto&& exeTry) mutable {
                    if (exeTry.hasException()) {
                        return notifyError(pros, Status::Error(exeTry.exception().what()));
                    }
                    auto exeStatus = std::move(exeTry).value();
                    if (!exeStatus.ok()) {
                        return notifyError(pros, exeStatus);
                    }
                    return notifyOK(pros);
                });
        });
}

void AsyncMsgNotifyBasedScheduler::runLeafExecutor(
    Executor* exe,
    folly::Executor* runner,
    std::vector<folly::Promise<Status>>&& promises) const {
    std::move(execute(exe))
        .via(runner)
        .thenTry([pros = std::move(promises), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(pros, Status::Error(t.exception().what()));
            }
            auto s = std::move(t).value();
            if (!s.ok()) {
                return notifyError(pros, s);
            }
            return notifyOK(pros);
        });
}

void AsyncMsgNotifyBasedScheduler::runLoop(std::vector<folly::Future<Status>>&& futures,
                                            LoopExecutor* loop,
                                            folly::Executor* runner,
                                            std::vector<folly::Promise<Status>>&& promises) const {
    folly::collect(futures).via(runner).thenTry(
        [loop, runner, pros = std::move(promises), this](auto&& t) mutable {
            if (t.hasException()) {
                return notifyError(pros, Status::Error(t.exception().what()));
            }
            auto status = std::move(t).value();
            auto s = checkStatus(std::move(status));
            if (!s.ok()) {
                return notifyError(pros, s);
            }

            std::move(execute(loop)).thenTry(
                [loop, runner, pros = std::move(pros), this](auto&& loopTry) mutable {
                    if (loopTry.hasException()) {
                        return notifyError(pros, Status::Error(loopTry.exception().what()));
                    }
                    auto loopStatus = std::move(loopTry).value();
                    if (!loopStatus.ok()) {
                        return notifyError(pros, loopStatus);
                    }
                    auto val = qctx_->ectx()->getValue(loop->node()->outputVar());
                    if (!val.isBool()) {
                        std::stringstream ss;
                        ss << "Loop produces a bad condition result: " << val
                           << " type: " << val.type();
                        return notifyError(pros, Status::Error(ss.str()));
                    }
                    if (val.getBool()) {
                        auto loopBody = loop->loopBody();
                        auto scheduleFuture = doSchedule(loopBody);
                        std::vector<folly::Future<Status>> fs;
                        fs.emplace_back(std::move(scheduleFuture));
                        runLoop(std::move(fs), loop, runner, std::move(pros));
                    } else {
                        return notifyOK(pros);
                    }
                });
        });
}

Status AsyncMsgNotifyBasedScheduler::checkStatus(std::vector<Status>&& status) const {
    for (auto& s : status) {
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

void AsyncMsgNotifyBasedScheduler::notifyOK(std::vector<folly::Promise<Status>>& promises) const {
    for (auto& p : promises) {
        p.setValue(Status::OK());
    }
}

void AsyncMsgNotifyBasedScheduler::notifyError(std::vector<folly::Promise<Status>>& promises,
                                               Status status) const {
    for (auto& p : promises) {
        p.setValue(status);
    }
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::execute(Executor *executor) const {
    auto status = executor->open();
    if (!status.ok()) {
        return executor->error(std::move(status));
    }
    return executor->execute().thenValue([executor](Status s) {
        NG_RETURN_IF_ERROR(s);
        return executor->close();
    });
}

}  // namespace graph
}  // namespace nebula
