/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "thread/GenericWorker.h"

#ifndef EV_MULTIPLICITY
#define EV_MULTIPLICITY 1
#endif
#include <ev.h>

namespace vesoft {
namespace thread {

GenericWorker::GenericWorker() {
}

GenericWorker::~GenericWorker() {
    stop();
    wait();
    if (notifier_ != nullptr) {
        notifier_ = nullptr;
    }
    if (evloop_ != nullptr) {
        ev_loop_destroy(evloop_);
        evloop_ = nullptr;
    }
}

bool GenericWorker::start(std::string name) {
    name_ = std::move(name);
    if (!stopped_.load(std::memory_order_acquire)) {
        return false;
    }
    evloop_ = ev_loop_new(0);
    ev_set_userdata(evloop_, this);

    auto cb = [] (struct ev_loop *loop, ev_async *, int) {
        reinterpret_cast<GenericWorker*>(ev_userdata(loop))->onNotify();
    };
    notifier_  = std::make_unique<ev_async>();
    ev_async_init(notifier_.get(), cb);
    ev_async_start(evloop_, notifier_.get());

    thread_ = std::make_unique<NamedThread>(name_, &GenericWorker::loop, this);

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
    ev_run(evloop_, 0);
}

void GenericWorker::notify() {
    if (notifier_ == nullptr) {
        return;
    }
    ev_async_send(evloop_, notifier_.get());
}

void GenericWorker::onNotify() {
    if (stopped_.load(std::memory_order_acquire)) {
        ev_break(evloop_, EVBREAK_ALL);
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
        auto cb = [] (struct ev_loop *loop, ev_timer *w, int) {
            auto timer = reinterpret_cast<Timer*>(w->data);
            auto worker = reinterpret_cast<GenericWorker*>(ev_userdata(loop));
            timer->callback_();
            if (timer->intervalSec_ == 0.0) {
                worker->purgeTimerInternal(timer->id_);
            } else {
                w->repeat = timer->intervalSec_;
                ev_timer_again(loop, w);
            }
        };
        for (auto &timer : newcomings) {
            timer->ev_ = std::make_unique<ev_timer>();
            auto delay = timer->delaySec_;
            auto interval = timer->intervalSec_;
            ev_timer_init(timer->ev_.get(), cb, delay, interval);
            timer->ev_->data = timer.get();
            ev_timer_start(evloop_, timer->ev_.get());
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
}

void GenericWorker::purgeTimerTask(uint64_t id) {
    {
        std::lock_guard<std::mutex> guard(lock_);
        purgingingTimers_.push_back(id);
    }
    notify();
}

void GenericWorker::purgeTimerInternal(uint64_t id) {
    auto iter = activeTimers_.find(id);
    if (iter != activeTimers_.end()) {
        ev_timer_stop(evloop_, iter->second->ev_.get());
        activeTimers_.erase(iter);
    }
}

}   // namespace thread
}   // namespace vesoft

