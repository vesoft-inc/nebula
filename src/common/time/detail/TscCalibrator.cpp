/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

namespace nebula {
namespace time {

void launchTickTockThread();
/**
 * TODO(dutor)
 * Please note that there is no guarantee that `__dumy' will be instantiated
 * after those global variables in TscHelper.cpp.
 * God bless us the linking order and the `sleep(2)' may help.
 * I will address this by refactoring or rewriting these whole things.
 */
struct __Dumy {
    __Dumy() {
        launchTickTockThread();
        // re-launch tick-tock thread after forking
        ::pthread_atfork(nullptr, nullptr, &launchTickTockThread);
    }
} __dumy;

}   // namespace time
}   // namespace nebula
