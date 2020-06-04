/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_SIGNALHANDLER_H_
#define COMMON_BASE_SIGNALHANDLER_H_

#include "common/base/Base.h"
#include <signal.h>
#include "common/base/Status.h"

/**
 * SignalHandler is a singleton to do the basic signal hanling,
 * mainly used in a daemon executable.
 *
 * By default, it ignores SIGPIPE and SIGHUP as we usually do.
 *
 * In the signal handler's context, we try our best to avoid memory allocation,
 * since this is the major reason which results in async-signal-unsafe.
 *
 * We strongly suggest not to handle fatal signals by yourself,
 * just let the program die and generate a coredump file.
 */

namespace nebula {

class SignalHandler final {
public:
    ~SignalHandler() = default;

    /**
     * To install one or several signals to handle.
     * Upon any signal arrives, the cooresponding handler would be invoked,
     * with an argument holding the informations about the signal and the sender.
     * The handler typically prints out the info and do some other things,
     * e.g. stop the process on SIGTERM.
     */
    class GeneralSignalInfo;
    using Handler = std::function<void(GeneralSignalInfo*)>;
    static Status install(int sig, Handler handler);
    static Status install(std::initializer_list<int> sigs, Handler handler);

    class GeneralSignalInfo {
    public:
        explicit GeneralSignalInfo(const siginfo_t *info);
        virtual ~GeneralSignalInfo() = default;
        virtual const char* toString() const;
        int sig() const { return sig_; }
        pid_t pid() const { return pid_; }
        uid_t uid() const { return uid_; }

    protected:
        int                                     sig_{0};
        pid_t                                   pid_{0};
        uid_t                                   uid_{0};
    };

    // TODO(dutor) Retrieve the stacktrace of the thread who raises the fatal signal
    class FatalSignalInfo final : public GeneralSignalInfo {
    public:
        FatalSignalInfo(const siginfo_t *info, void *uctx);
        ~FatalSignalInfo() override = default;
        const char *toString() const override;
    };

private:
    SignalHandler();
    SignalHandler(const SignalHandler&) = delete;
    SignalHandler& operator=(const SignalHandler&) = delete;
    SignalHandler(SignalHandler&&) = delete;
    SignalHandler& operator=(SignalHandler&&) = delete;

    // Get the singleton
    static SignalHandler& get();

    // The handler we use to register to the operating system.
    static void handlerHook(int sig, siginfo_t *info, void *uctx);

    // init SIGPIPE and SIGHUB
    Status init();

    // Invoked by handlerHook, and dispatch signals to handleGeneralSignal or handleFatalSignal
    void doHandle(int sig, siginfo_t *info, void *uctx);

    // To handle general signals like SIGINT, SIGTERM, etc.
    void handleGeneralSignal(int sig, siginfo_t *info);

    // To handle fatal signals like SIGSEGV, SIGABRT, etc.
    void handleFatalSignal(int sig, siginfo_t *info, void *uctx);

    Status installInternal(int sig, Handler handler);

private:
    // The POSIX signal number ranges from 1 to 64, so we use (sig - 1) as index
    std::array<Handler, 64>                     handlers_;
};


inline std::ostream&
operator<<(std::ostream &os, const SignalHandler::GeneralSignalInfo &info) {
    return os << info.toString();
}

}   // namespace nebula


#endif  // COMMON_BASE_SIGNALHANDLER_H_
