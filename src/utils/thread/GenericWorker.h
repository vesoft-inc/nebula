/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_THREAD_GENERICWORKER_H_
#define COMMON_THREAD_GENERICWORKER_H_

#include "common/base/Base.h"
#include <folly/futures/Future.h>
#include <folly/Unit.h>
#include "common/cpp/helpers.h"
#include "common/thread/NamedThread.h"

/**
 * GenericWorker implements a event-based task executor that executes tasks asynchronously
 * in a separate thread. Like `std::thread', It takes any callable object and its optional
 * arguments as a normal, delayed or repeated task.
 *
 * GenericWorker executes tasks one after one, in the FIFO way, while tasks are non-preemptible.
 *
 * Please NOTE that, as the name indicates, this a worker thread for the general purpose,
 * but not for the performance critical situation.
 */

struct event;
struct event_base;

namespace nebula {
namespace thread {

class GenericWorker final : public nebula::cpp::NonCopyable, public nebula::cpp::NonMovable {
public:
    friend class GenericThreadPool;

    GenericWorker();
    ~GenericWorker();

    /**
     * To allocate resouces and launch the internal thread which executes
     * the event loop to make this worker usable.
     *
     * Optionally, you could give the internal thread a specific name,
     * which will be shown in some system utilities like `top'.
     *
     * Once started, the worker will keep waiting for newly-added tasks indefinitely,
     * until `stop' is invoked.
     *
     * A GenericWorker MUST be `start'ed successfully before invoking
     * any other interfaces.
     */
    bool MUST_USE_RESULT start(std::string name = "");

    /**
     * Asynchronouly to notify the worker to stop handling further new tasks.
     */
    bool stop();

    /**
     * Synchronously to wait the worker to finish and exit.
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
    auto addTask(F &&task, Args &&...args)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type;
    template <typename F, typename...Args>
    auto addTask(F &&task, Args &&...args)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
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
    auto addDelayTask(size_t ms, F &&task, Args &&...args)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type;
    template <typename F, typename...Args>
    auto addDelayTask(size_t ms, F &&task, Args &&...args)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
           >::type;

    /**
     * To add a repeated timer task which will be executed in each period.
     * @ms      interval in milliseconds
     * @task    a callable object
     * @args    variadic arguments
     * @return  ID of the added task, unique for this worker
     */
    template <typename F, typename...Args>
    uint64_t addRepeatTask(size_t ms, F &&task, Args &&...args);

    /**
     * To purge or deactivate a repeated task.
     * @id      ID returned by `addRepeatTask'
     */
    void purgeTimerTask(uint64_t id);

    template <typename F, typename...Args>
    uint64_t addTimerTask(size_t, size_t, F&&, Args&&...);

private:
    void purgeTimerInternal(uint64_t id);

private:
    struct Timer {
        explicit Timer(std::function<void(void)> cb);
        ~Timer();
        uint64_t                                id_;
        uint64_t                                delayMSec_;
        uint64_t                                intervalMSec_;
        std::function<void(void)>               callback_;
        struct event                           *ev_{nullptr};
        GenericWorker                          *owner_{nullptr};
    };

private:
    void loop();
    void notify();
    void onNotify();
    uint64_t nextTimerId() {
        // !NOTE! `lock_' must be hold
        return (nextTimerId_++ & TIMER_ID_MASK);
    }

private:
    static constexpr uint64_t TIMER_ID_BITS     = 6 * 8;
    static constexpr uint64_t TIMER_ID_MASK     = ((~0x0UL) >> (64 - TIMER_ID_BITS));
    std::string                                 name_;
    std::atomic<bool>                           stopped_{true};
    volatile uint64_t                           nextTimerId_{0};
    struct event_base                          *evbase_ = nullptr;
    int                                         evfd_ = -1;
    struct event                               *notifier_ = nullptr;
    std::mutex                                  lock_;
    std::vector<std::function<void()>>          pendingTasks_;
    using TimerPtr = std::unique_ptr<Timer>;
    std::vector<TimerPtr>                       pendingTimers_;
    std::vector<uint64_t>                       purgingingTimers_;
    std::unordered_map<uint64_t, TimerPtr>      activeTimers_;
    std::unique_ptr<NamedThread>                thread_;
};


template <typename F, typename...Args>
auto GenericWorker::addTask(F &&f, Args &&...args)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type {
    auto promise = std::make_shared<folly::Promise<folly::Unit>>();
    auto task = std::make_shared<std::function<ReturnType<F, Args...> ()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    auto future = promise->getSemiFuture();
    {
        std::lock_guard<std::mutex> guard(lock_);
        pendingTasks_.emplace_back([=] {
            try {
                (*task)();
                promise->setValue(folly::unit);
            } catch (const std::exception& ex) {
                promise->setException(ex);
            }
        });
    }
    notify();
    return future;
}


template <typename F, typename...Args>
auto GenericWorker::addTask(F &&f, Args &&...args)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
           >::type {
    auto promise = std::make_shared<folly::Promise<ReturnType<F, Args...>>>();
    auto task = std::make_shared<std::function<ReturnType<F, Args...> ()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    auto future = promise->getSemiFuture();
    {
        std::lock_guard<std::mutex> guard(lock_);
        pendingTasks_.emplace_back([=] {
            promise->setWith(*task);
        });
    }
    notify();
    return future;
}


template <typename F, typename...Args>
auto GenericWorker::addDelayTask(size_t ms, F &&f, Args &&...args)
        -> typename std::enable_if<
            std::is_void<ReturnType<F, Args...>>::value,
            UnitFutureType
           >::type {
    auto promise = std::make_shared<folly::Promise<folly::Unit>>();
    auto task = std::make_shared<std::function<ReturnType<F, Args...> ()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    auto future = promise->getSemiFuture();
    addTimerTask(ms, 0, [=] {
        try {
            (*task)();
            promise->setValue(folly::unit);
        } catch (const std::exception& ex) {
            promise->setException(ex);
        }
    });
    return future;
}


template <typename F, typename...Args>
auto GenericWorker::addDelayTask(size_t ms, F &&f, Args &&...args)
        -> typename std::enable_if<
            !std::is_void<ReturnType<F, Args...>>::value,
            FutureType<F, Args...>
           >::type {
    auto promise = std::make_shared<folly::Promise<ReturnType<F, Args...>>>();
    auto task = std::make_shared<std::function<ReturnType<F, Args...> ()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    auto future = promise->getSemiFuture();
    addTimerTask(ms, 0, [=] {
        promise->setWith(*task);
    });
    return future;
}


template <typename F, typename...Args>
uint64_t GenericWorker::addRepeatTask(size_t ms, F &&f, Args &&...args) {
    return addTimerTask(ms,
                        ms,
                        std::forward<F>(f),
                        std::forward<Args>(args)...);
}


template <typename F, typename...Args>
uint64_t GenericWorker::addTimerTask(size_t delay,
                                     size_t interval,
                                     F &&f,
                                     Args &&...args) {
    auto timer = std::make_unique<Timer>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    timer->delayMSec_ = delay;
    timer->intervalMSec_ = interval;
    timer->owner_ = this;
    auto id = 0UL;
    {
        std::lock_guard<std::mutex> guard(lock_);
        timer->id_ = (id = nextTimerId());
        pendingTimers_.emplace_back(std::move(timer));
    }
    notify();
    return id;
}

}   // namespace thread
}   // namespace nebula

#endif  // COMMON_THREAD_GENERICWORKER_H_
