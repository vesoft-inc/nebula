/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/thread/GenericWorker.h"
#include <sys/eventfd.h>
#include <event2/event.h>

namespace nebula {
namespace thread {

GenericWorker::GenericWorker() = default;

GenericWorker::~GenericWorker() {
    stop();
    wait();
    if (notifier_ != nullptr) {
        event_free(notifier_);
        notifier_ = nullptr;
    }
    if (evbase_ != nullptr) {
        event_base_free(evbase_);
        evbase_ = nullptr;
    }
    if (evfd_ >= 0) {
        ::close(evfd_);
    }
}

bool GenericWorker::start(std::string name) {
    if (!stopped_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "GenericWroker already started";
        return false;
    }
    name_ = std::move(name);

    // Create an event base
    evbase_ = event_base_new();
    DCHECK(evbase_ != nullptr);

    // Create an eventfd for async notification
    evfd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evfd_ == -1) {
        LOG(ERROR) << "Create eventfd failed: " << ::strerror(errno);
        return false;
    }
    auto cb = [] (int fd, int16_t, void *arg) {
        auto val = 0UL;
        auto len = ::read(fd, &val, sizeof(val));
        DCHECK(len == sizeof(val));
        reinterpret_cast<GenericWorker*>(arg)->onNotify();
    };
    auto events = EV_READ | EV_PERSIST;
    notifier_  = event_new(evbase_, evfd_, events, cb, this);
    DCHECK(notifier_ != nullptr);
    event_add(notifier_, nullptr);

    // Launch a new thread to run the event loop
    thread_ = std::make_unique<NamedThread>(name_, &GenericWorker::loop, this);

    // Mark this worker as started
    stopped_.store(false, std::memory_order_release);

    return true;
}

bool GenericWorker::stop() {
    if (stopped_.load(std::memory_order_acquire)) {
        return false;
    }
    stopped_.store(true, std::memory_order_release);
    notify();
    return true;
}

bool GenericWorker::wait() {
    if (thread_ == nullptr) {
        return false;
    }
    thread_->join();
    thread_.reset();
    return true;
}

void GenericWorker::loop() {
    event_base_dispatch(evbase_);
}

void GenericWorker::notify() {
    if (notifier_ == nullptr) {
        return;
    }
    DCHECK_NE(-1, evfd_);
    auto one = 1UL;
    auto len = ::write(evfd_, &one, sizeof(one));
    DCHECK(len == sizeof(one));
}

void GenericWorker::onNotify() {
    if (stopped_.load(std::memory_order_acquire)) {
        event_base_loopexit(evbase_, nullptr);
        // Even been broken, we still fall through to finish the current loop.
    }
    {
        decltype(pendingTasks_) newcomings;
        {
            std::lock_guard<std::mutex> guard(lock_);
            newcomings.swap(pendingTasks_);
        }
        for (auto &task : newcomings) {
            task();
        }
    }
    {
        decltype(pendingTimers_) newcomings;
        {
            std::lock_guard<std::mutex> guard(lock_);
            newcomings.swap(pendingTimers_);
        }
        auto cb = [] (int fd, int16_t, void *arg) {
            UNUSED(fd);
            auto timer = reinterpret_cast<Timer*>(arg);
            auto worker = timer->owner_;
            timer->callback_();
            if (timer->intervalMSec_ == 0.0) {
                worker->purgeTimerInternal(timer->id_);
            }
        };
        for (auto &timer : newcomings) {
            timer->ev_ = event_new(evbase_, -1, EV_PERSIST, cb, timer.get());

            auto delay = timer->delayMSec_;
            struct timeval tv;
            tv.tv_sec = delay / 1000;
            tv.tv_usec = delay % 1000 * 1000;
            evtimer_add(timer->ev_, &tv);

            auto id = timer->id_;
            activeTimers_[id] = std::move(timer);
        }
    }
    {
        decltype(purgingingTimers_) newcomings;
        {
            std::lock_guard<std::mutex> guard(lock_);
            newcomings.swap(purgingingTimers_);
        }
        for (auto id : newcomings) {
            purgeTimerInternal(id);
        }
    }
}

GenericWorker::Timer::Timer(std::function<void(void)> cb) {
    callback_ = std::move(cb);
}

GenericWorker::Timer::~Timer() {
    if (ev_ != nullptr) {
        event_free(ev_);
    }
}

void GenericWorker::purgeTimerTask(uint64_t id) {
    {
        std::lock_guard<std::mutex> guard(lock_);
        purgingingTimers_.emplace_back(id);
    }
    notify();
}

void GenericWorker::purgeTimerInternal(uint64_t id) {
    auto iter = activeTimers_.find(id);
    if (iter != activeTimers_.end()) {
        evtimer_del(iter->second->ev_);
        activeTimers_.erase(iter);
    }
}

}   // namespace thread
}   // namespace nebula
