/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/base/SignalHandler.h"

namespace nebula {

SignalHandler::SignalHandler() {
}


Status SignalHandler::init() {
    // Ignore SIGPIPE and SIGHUP
    auto ignored = {SIGPIPE, SIGHUP};
    for (auto sig : ignored) {
        struct sigaction act;
        ::memset(&act, 0, sizeof(act));
        act.sa_handler = SIG_IGN;
        if (0 != ::sigaction(sig, &act, nullptr)) {
            return Status::Error("Register signal %d failed: %s", sig, ::strerror(errno));
        }
    }

    return Status::OK();
}


SignalHandler& SignalHandler::get() {
    static SignalHandler instance;
    return instance;
}


// static
Status SignalHandler::install(int sig, Handler handler) {
    static bool init_ = false;
    if (!init_) {
        auto status = get().init();
        if (!status.ok()) {
            return status;
        }
        init_ = true;
    }

    CHECK(sig >= 1 && sig <= 64);
    CHECK(sig != SIGKILL) << "SIGKILL cannot be handled";
    CHECK(sig != SIGSTOP) << "SIGKSTOP cannot be handled";
    return get().installInternal(sig, std::move(handler));
}


// static
Status SignalHandler::install(std::initializer_list<int> sigs, Handler handler) {
    auto status = Status::OK();
    for (auto sig : sigs) {
        status = install(sig, handler);
        if (!status.ok()) {
            break;
        }
    }
    return status;
}


Status SignalHandler::installInternal(int sig, Handler handler) {
    struct sigaction act;
    ::memset(&act, 0, sizeof(act));
    act.sa_sigaction = &handlerHook;
    act.sa_flags |= SA_SIGINFO;
    if (::sigaction(sig, &act, nullptr) != 0) {
        return Status::Error("Register signal %d failed: %s", sig, ::strerror(errno));
    }
    auto index = sig - 1;
    if (handlers_[index]) {
        LOG(WARNING) << "Register signal " << sig << " twice!";
    }
    handlers_[index] = std::move(handler);
    return Status::OK();
}


// static
void SignalHandler::handlerHook(int sig, siginfo_t *info, void *uctx) {
    get().doHandle(sig, info, uctx);
}


void SignalHandler::doHandle(int sig, siginfo_t *info, void *uctx) {
    switch (sig) {
        case SIGSEGV:   // segment fault
        case SIGABRT:   // abort
        case SIGILL:    // ill instruction
        case SIGFPE:    // floating point error, e.g. divide by zero
        case SIGBUS:    // I/O error in mmaped memory, mce error, etc.
            handleFatalSignal(sig, info, uctx);
            break;
        case SIGCHLD:
            // TODO(dutor) Since we rarely use this signal, we regard it as a general one for now.
            handleGeneralSignal(sig, info);
            break;
    }

    handleGeneralSignal(sig, info);
}


void SignalHandler::handleGeneralSignal(int sig, siginfo_t *info) {
    auto index = sig - 1;
    GeneralSignalInfo siginfo(info);
    handlers_[index](&siginfo);
}


void SignalHandler::handleFatalSignal(int sig, siginfo_t *info, void *uctx) {
    auto index = sig - 1;
    FatalSignalInfo siginfo(info, uctx);
    handlers_[index](&siginfo);
    // Restore the signal handler to its default to make the program crash
    ::signal(sig, SIG_DFL);
    ::raise(sig);
}


SignalHandler::GeneralSignalInfo::GeneralSignalInfo(const siginfo_t *info) {
    pid_ = info->si_pid;
    uid_ = info->si_uid;
    sig_ = info->si_signo;
}


const char* SignalHandler::GeneralSignalInfo::toString() const {
    static thread_local char buffer[1024];
    snprintf(buffer, sizeof(buffer),
            "sig[%d], name[%s], pid[%d], uid(%d)",
            sig_, ::strsignal(sig_), pid_, uid_);
    return buffer;
}


SignalHandler::FatalSignalInfo::FatalSignalInfo(const siginfo_t *info, void *uctx)
    : GeneralSignalInfo(info) {
    UNUSED(uctx);
}


const char* SignalHandler::FatalSignalInfo::toString() const {
    return GeneralSignalInfo::toString();
}

}   // namespace nebula
