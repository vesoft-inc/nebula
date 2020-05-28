/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_THREAD_GENERICTHREADPOOL_H_
#define COMMON_THREAD_GENERICTHREADPOOL_H_

#include "common/thread/GenericWorker.h"

/**
 * Based on GenericWorker, GenericThreadPool implements a thread pool that execute tasks asynchronously.
 *
 * Under the hood, GenericThreadPool distributes tasks around the internal threads in a round-robin way.
 *
 * Please NOTE that, as the name indicates, this a thread pool for the general purpose,
 * but not for the performance critical situation.
 */

namespace nebula {
namespace thread {

class GenericThreadPool final : public nebula::cpp::NonCopyable
                              , public nebula::cpp::NonMovable {
public:
    GenericThreadPool();
    ~GenericThreadPool();

    /**
     * To launch the internal generic workers.
     *
     * Optionally, you could give the internal thread a specific name,
     * which will be shown in some system utilities like `top'.
     *
     * Once started, the worker will keep waiting for newly-added tasks indefinitely,
     * until `stop' is invoked.
     *
     * A GenericThreadPool MUST be `start'ed successfully before invoking
     * any other interfaces.
     *
     * @nrThreads   number of internal threads
     * @name        name of internal threads
     */
    bool start(size_t nrThreads, const std::string &name = "");

    /**
     * Asynchronouly to notify the workers to stop handling further new tasks.
     */
    bool stop();

    /**
     * Synchronously to wait the workers to finish and exit.
     *
     * For the sake of convenience, `~GenericWorker' invokes `stop' and `wait',
     * but it's better to control these processes manually to make the resource
     * management more clearly.
     */
    bool wait();

    template <typename F, typename...Args>
    using ReturnType = typename std::result_of<F(Args...)>::type;
    template <typename F, typename...Args>
    using FutureType = folly::SemiFuture<ReturnType<F, Args...>>;
    using UnitFutureType = folly::SemiFuture<folly::Unit>;

    /**
     * To add a normal task.
     * @task    a callable object
     * @args    variadic arguments
     * @return  an instance of `folly::SemiFuture' you could wait upon
     *          for the result of `task'
     */
    template <typename F, typename...Args>
    auto addTask(F&&, Args&&...)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
           >::type;
    template <typename F, typename...Args>
    auto addTask(F&&, Args&&...)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type;

    /**
     * To add a oneshot timer task which will be executed after a while.
     * @ms      milliseconds from now when the task get executed
     * @task    a callable object
     * @args    variadic arguments
     * @return  an instance of `folly::SemiFuture' you could wait upon
     *          for the result of `task'
     */
    template <typename F, typename...Args>
    auto addDelayTask(size_t, F&&, Args&&...)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
           >::type;
    template <typename F, typename...Args>
    auto addDelayTask(size_t, F&&, Args&&...)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type;

    /**
     * To add a repeated timer task which will be executed in each period.
     * @ms      interval in milliseconds
     * @task    a callable object
     * @args    variadic arguments
     * @return  ID of the added task, unique for this worker
     */
    template <typename F, typename...Args>
    uint64_t addRepeatTask(size_t, F&&, Args&&...);

    /**
     * To purge or deactivate a repeated task.
     * @id      ID returned by `addRepeatTask'
     */
    void purgeTimerTask(uint64_t id);

private:
    size_t                                          nrThreads_{0};
    std::atomic<size_t>                             nextThread_{0};
    std::vector<std::unique_ptr<GenericWorker>>     pool_;
};


template <typename F, typename...Args>
auto GenericThreadPool::addTask(F &&f, Args &&...args)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
           >::type {
    auto idx = nextThread_++ % nrThreads_;
    return pool_[idx]->addTask(std::forward<F>(f),
                               std::forward<Args>(args)...);
}


template <typename F, typename...Args>
auto GenericThreadPool::addTask(F &&f, Args &&...args)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type {
    auto idx = nextThread_++ % nrThreads_;
    return pool_[idx]->addTask(std::forward<F>(f),
                               std::forward<Args>(args)...);
}


template <typename F, typename...Args>
auto GenericThreadPool::addDelayTask(size_t ms, F &&f, Args &&...args)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
           >::type {
    auto idx = nextThread_++ % nrThreads_;
    return pool_[idx]->addDelayTask(ms,
                                    std::forward<F>(f),
                                    std::forward<Args>(args)...);
}


template <typename F, typename...Args>
auto GenericThreadPool::addDelayTask(size_t ms, F &&f, Args &&...args)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type {
    auto idx = nextThread_++ % nrThreads_;
    return pool_[idx]->addDelayTask(ms,
                                    std::forward<F>(f),
                                    std::forward<Args>(args)...);
}


template <typename F, typename...Args>
uint64_t GenericThreadPool::addRepeatTask(size_t ms, F &&f, Args &&...args) {
    auto idx = nextThread_++ % nrThreads_;
    auto id = pool_[idx]->addRepeatTask(ms,
                                        std::forward<F>(f),
                                        std::forward<Args>(args)...);
    return ((idx << GenericWorker::TIMER_ID_BITS) | id);
}

}   // namespace thread
}   // namespace nebula

#endif  // COMMON_THREAD_GENERICTHREADPOOL_H_
