/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_SINGALUTILS_H
#define NEBULA_GRAPH_SINGALUTILS_H

#include "base/Base.h"
#include <signal.h>

#define SYSTEM_SIGNAL_SETUP(daemon_name)                            \
    using nebula::signalutils::SignalHandler;                       \
    LOG(INFO) << "begin setup signal for daemon : " << daemon_name; \
    {                                                               \
       SignalHandler s0(SIGINT, &exit_signal_handler);              \
       SignalHandler s1(SIGSEGV, &exit_signal_handler);             \
       SignalHandler s2(SIGABRT, &exit_signal_handler);             \
       SignalHandler s3(SIGHUP, &exit_signal_handler);              \
       SignalHandler s4(SIGQUIT, &exit_signal_handler);             \
       SignalHandler s5(SIGTERM, &exit_signal_handler);             \
       SignalHandler s6(SIGALRM, &ignore_signal_handler);           \
       SignalHandler s7(SIGFPE, &ignore_signal_handler);            \
       SignalHandler s8(SIGPIPE, &ignore_signal_handler);           \
    }

void ignore_signal_handler(int signal_num) {
    LOG(WARNING) << "Signal [" << signal_num << "] is captured, but ignored";
}

namespace nebula {
namespace signalutils {
//==================================================================================================
///   Handles a given signal by calling the callback .
//==================================================================================================
class SignalHandler {
private:
    typedef void(*HandlerType)(int);

    struct HandlerData {
        HandlerType handler;
    };

    typedef std::unordered_map<int, HandlerData> SignalMap;

public:
    //================================================================================================
    //    Constructor.
    //    @param  signal    The signal to handle.
    //    @param  handler   The user's signal handler.
    //================================================================================================
    SignalHandler(int signal, HandlerType handler) {
        HandlerData h;
        h.handler = handler;
        SignalHandler::signalMap[signal] = h;
        ::signal(signal, &SignalHandler::handleAll);
    }

private:
    //================================================================================================
    //    The first signal callback. Looks for the user defined handler and calls that with the
    //    signal.
    //    @param  signal    The signal to handle.
    //================================================================================================
    static void handleAll(int signal) {
        HandlerData h = SignalHandler::signalMap[signal];
        h.handler(signal);
    }

    static SignalMap signalMap;
};

SignalHandler::SignalMap SignalHandler::signalMap;

}  // namespace signalutils
}  // namespace nebula

#endif  // NEBULA_GRAPH_SINGALUTILS_H
