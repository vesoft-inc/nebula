/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>  // for TestPartResult
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartResult
#include <signal.h>       // for raise, SIGHUP, SIGTERM, SIGPIPE
#include <unistd.h>       // for getpid, getuid

#include <memory>  // for allocator

#include "common/base/SignalHandler.h"  // for SignalHandler

namespace nebula {

TEST(SignalHandler, Term) {
  auto handler = [](auto *info) {
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
  auto handler = [](auto *info) {
    ASSERT_EQ(SIGHUP, info->sig());
    ASSERT_EQ(::getpid(), info->pid());
    ASSERT_EQ(::getuid(), info->uid());
  };
  SignalHandler::install({SIGHUP}, handler);
  SignalHandler::install({SIGHUP}, handler);
  ::raise(SIGHUP);
}

}  // namespace nebula
