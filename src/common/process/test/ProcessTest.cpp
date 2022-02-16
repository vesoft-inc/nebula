/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <errno.h>             // for ESRCH, errno
#include <folly/Random.h>      // for Random
#include <folly/Range.h>       // for StringPiece
#include <folly/ScopeGuard.h>  // for operator+, SCOPE_EXIT
#include <folly/String.h>      // for stringPrintf, rtrimWhitespace
#include <gtest/gtest.h>       // for Message
#include <gtest/gtest.h>       // for TestPartResult
#include <gtest/gtest.h>       // for Message
#include <gtest/gtest.h>       // for TestPartResult
#include <limits.h>            // for PATH_MAX
#include <signal.h>            // for kill
#include <stdint.h>            // for uint32_t
#include <stdlib.h>            // for getenv
#include <unistd.h>            // for getpid, unlink, getcwd, rmdir

#include <fstream>  // for ifstream, fpos, basic_istre...
#include <string>   // for basic_string, allocator

#include "common/base/Base.h"             // for UNUSED
#include "common/base/Status.h"           // for operator<<, Status
#include "common/base/StatusOr.h"         // for StatusOr
#include "common/fs/FileUtils.h"          // for FileUtils
#include "common/process/ProcessUtils.h"  // for ProcessUtils

namespace nebula {

TEST(ProcessUtils, getExePath) {
  auto result = ProcessUtils::getExePath();
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_NE(std::string::npos, result.value().find("process_test")) << result.value();
}

TEST(ProcessUtils, getExeCWD) {
  auto result = ProcessUtils::getExeCWD();
  ASSERT_TRUE(result.ok()) << result.status();
  char buffer[PATH_MAX];
  auto len = ::getcwd(buffer, sizeof(buffer));
  UNUSED(len);
  ASSERT_EQ(buffer, result.value());
}

TEST(ProcessUtils, isPidAvailable) {
  {
    auto status = ProcessUtils::isPidAvailable(::getpid());
    ASSERT_TRUE(status.ok()) << status;
  }
  {
    auto status = ProcessUtils::isPidAvailable(0);  // idle/swap
    ASSERT_FALSE(status.ok());
  }
  {
    auto status = ProcessUtils::isPidAvailable(1);  // systemd
    ASSERT_FALSE(status.ok());
  }
  {
    // pid file which contains pid of current process
    auto pidFile = folly::stringPrintf("/tmp/non-existing-dir-%d/process_test.pid", ::getpid());
    SCOPE_EXIT {
      ::unlink(pidFile.c_str());
      ::rmdir(fs::FileUtils::dirname(pidFile.c_str()).c_str());
    };
    auto status = ProcessUtils::makePidFile(pidFile);
    ASSERT_TRUE(status.ok()) << status;
    status = ProcessUtils::isPidAvailable(pidFile);
    ASSERT_TRUE(status.ok()) << status;
  }
  {
    // pid file not exist
    auto pidFile = "/tmp/definitely-not-exist.pid";
    auto status = ProcessUtils::isPidAvailable(pidFile);
    ASSERT_TRUE(status.ok()) << status;
  }
  {
    // choose an available pid
    auto genPid = []() {
      auto max = ProcessUtils::maxPid();
      while (true) {
        uint32_t next = static_cast<uint32_t>(folly::Random::rand64());
        next %= max;
        if (::kill(next, 0) == -1 && errno == ESRCH) {
          return next;
        }
      }
    };
    auto pidFile = folly::stringPrintf("/tmp/process_test-%d.pid", ::getpid());
    SCOPE_EXIT {
      ::unlink(pidFile.c_str());
    };
    auto status = ProcessUtils::makePidFile(pidFile, genPid());
    ASSERT_TRUE(status.ok()) << status;
    // there are chances that the chosen pid was occupied already,
    // but the chances are negligible, so be it.
    status = ProcessUtils::isPidAvailable(pidFile);
    ASSERT_TRUE(status.ok()) << status;
  }
}

TEST(ProcessUtils, getProcessName) {
  auto result = ProcessUtils::getProcessName();
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_NE(std::string::npos, result.value().find("process_test")) << result.value();
}

TEST(ProcessUtils, runCommand) {
  auto status1 = ProcessUtils::runCommand("echo $HOME");
  ASSERT_TRUE(status1.ok()) << status1.status();
  EXPECT_EQ(std::string(getenv("HOME")), folly::rtrimWhitespace(status1.value()).toString());

  // Try large output
  auto status2 = ProcessUtils::runCommand("cat /etc/profile");
  ASSERT_TRUE(status2.ok()) << status2.status();
  std::ifstream is("/etc/profile", std::ios::ate);
  auto size = is.tellg();
  EXPECT_EQ(size, status2.value().size());
  std::string buf(size, '\0');
  is.seekg(0);
  is.read(&buf[0], size);
  EXPECT_EQ(buf, status2.value());
}

}  // namespace nebula
