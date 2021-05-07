/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_SCHEDULER_H_
#define SCHEDULER_SCHEDULER_H_

#include <set>
#include <string>
#include <unordered_map>

#include <folly/SpinLock.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "common/base/Status.h"
#include "common/cpp/helpers.h"

namespace nebula {
namespace graph {

class Executor;
class QueryContext;
class LoopExecutor;

class Scheduler final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    // check whether a task is a Scheduler::Task by std::is_base_of<>::value in thread pool
    struct Task {
        int64_t planId;
        explicit Task(const Executor *e);
    };

    explicit Scheduler(QueryContext *qctx);
    ~Scheduler() = default;

    folly::Future<Status> schedule();

private:
    // Enable thread pool check the query plan id of each callback registered in future. The functor
    // is only the proxy of the invocable function `fn'.
    template <typename F>
    struct ExecTask : Task {
        using Extract = folly::futures::detail::Extract<F>;
        using Return = typename Extract::Return;
        using FirstArg = typename Extract::FirstArg;

        F fn;

        ExecTask(const Executor *e, F f) : Task(e), fn(std::move(f)) {}

        Return operator()(FirstArg &&arg) {
            return fn(std::forward<FirstArg>(arg));
        }
    };

    template <typename Fn>
    ExecTask<Fn> task(Executor *e, Fn &&f) const {
        return ExecTask<Fn>(e, std::forward<Fn>(f));
    }

    void analyze(Executor *executor);
    folly::Future<Status> doSchedule(Executor *executor);
    folly::Future<Status> doScheduleParallel(const std::set<Executor *> &dependents);
    folly::Future<Status> iterate(LoopExecutor *loop);
    folly::Future<Status> execute(Executor *executor);

    struct PassThroughData {
        folly::SpinLock lock;
        std::unique_ptr<folly::SharedPromise<Status>> promise;
        int32_t numOutputs;

        explicit PassThroughData(int32_t outputs);
    };

    QueryContext *qctx_{nullptr};
    std::unordered_map<int64_t, PassThroughData> passThroughPromiseMap_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_SCHEDULER_H_
