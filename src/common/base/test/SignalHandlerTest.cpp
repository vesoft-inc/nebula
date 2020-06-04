/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/base/SignalHandler.h"

namespace nebula {

TEST(SignalHandler, Term) {
    auto handler = [] (auto *info) {
        ASSERT_EQ(SIGTERM, info->sig());
        ASSERT_EQ(::getpid(), info->pid());
        ASSERT_EQ(::getuid(), info->uid());
    };
    SignalHandler::install({SIGTERM}, handler);
    ::raise(SIGTERM);
}


TEST(SignalHandler, Pipe) {
    // SIGPIPE has been ignored
    ::raise(SIGPIPE);
}


TEST(SignalHandler, Overwrite) {
    auto handler = [] (auto *info) {
        ASSERT_EQ(SIGHUP, info->sig());
        ASSERT_EQ(::getpid(), info->pid());
        ASSERT_EQ(::getuid(), info->uid());
    };
    SignalHandler::install({SIGHUP}, handler);
    SignalHandler::install({SIGHUP}, handler);
    ::raise(SIGHUP);
}

}   // namespace nebula
