/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef COMMON_THREAD_NAMEDTHREAD_H_
#define COMMON_THREAD_NAMEDTHREAD_H_

#include "base/Base.h"
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/syscall.h>

#include <sys/prctl.h>

namespace nebula {
namespace thread {

pid_t gettid();

class NamedThread final : public std::thread {
public:
    NamedThread() = default;
    NamedThread(NamedThread&&) = default;
    template <typename F, typename...Args>
    NamedThread(const std::string &name, F &&f, Args&&...args);
    NamedThread& operator=(NamedThread&&) = default;
    NamedThread(const NamedThread&) = delete;
    NamedThread& operator=(const NamedThread&) = delete;

    pid_t tid() const {
        while (tid_ == 0) {
            // `tid' is unavailable until the thread function is called.
        }
        return tid_;
    }

public:
    class Nominator {
    public:
        Nominator(const std::string &name) {
            get(prevName_);
            set(name);
        }

        ~Nominator() {
            set(prevName_);
        }

        static void set(const std::string &name) {
            ::prctl(PR_SET_NAME, name.c_str(), 0, 0, 0);
        }

        static void get(std::string &name) {
            char buf[64];
            ::prctl(PR_GET_NAME, buf, 0, 0, 0);
            name = buf;
        }

    private:
        std::string                     prevName_;
    };

private:
    static void hook(NamedThread *thread,
                     const std::string &name,
                     const std::function<void()> &f) {
        thread->tid_ = nebula::thread::gettid();
        if (!name.empty()) {
            Nominator::set(name);
        }
        f();
    }

private:
    pid_t                           tid_{0};
};

template <typename F, typename...Args>
NamedThread::NamedThread(const std::string &name, F &&f, Args&&...args)
    : std::thread(hook, this, name,
                  std::bind(std::forward<F>(f), std::forward<Args>(args)...)) {
};

}   // namespace thread
}   // namespace nebula

#endif  // COMMON_THREAD_NAMEDTHREAD_H_
