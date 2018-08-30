/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "common/concurrent/thread/NamedThread.h"

namespace vesoft {
namespace concurrent {

class TLSThreadID {
public:
    TLSThreadID() {
        tid_ = ::syscall(SYS_gettid);
    }

    ~TLSThreadID() {
        tid_ = 0;
    }

    pid_t tid() {
        return tid_;
    }

private:
    pid_t                           tid_;
};

pid_t gettid() {
    static thread_local TLSThreadID tlstid;
    return tlstid.tid();
}

}   // namespace concurrent
}   // namespace vesoft
