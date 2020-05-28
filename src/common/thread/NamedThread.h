/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_THREAD_NAMEDTHREAD_H_
#define COMMON_THREAD_NAMEDTHREAD_H_

#include "common/base/Base.h"
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

public:
    class Nominator {
    public:
        explicit Nominator(const std::string &name) {
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
    static void hook(const std::string &name,
                     const std::function<void()> &f) {
        if (!name.empty()) {
            Nominator::set(name);
        }
        f();
    }
};

template <typename F, typename...Args>
NamedThread::NamedThread(const std::string &name, F &&f, Args&&...args)
    : std::thread(hook, name,
                  std::bind(std::forward<F>(f), std::forward<Args>(args)...)) {
}

}   // namespace thread
}   // namespace nebula

#endif  // COMMON_THREAD_NAMEDTHREAD_H_
