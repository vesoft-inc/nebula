/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for TestPartResult
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult
#include <unistd.h>           // for usleep

#include <algorithm>  // for max, min
#include <memory>     // for unique_ptr, allocator, make...
#include <utility>    // for move

#include "common/base/Logging.h"          // for SetStderrLogging
#include "common/base/StatusOr.h"         // for StatusOr
#include "common/stats/StatsManager.h"    // for StatsManager::VT, StatsManager
#include "common/thread/GenericWorker.h"  // for GenericWorker

namespace nebula {
namespace stats {

TEST(StatsManager, RateTest) {
  auto statId = StatsManager::registerStats("ratetest", "AVG, SUM");
  auto thread = std::make_unique<thread::GenericWorker>();
  ASSERT_TRUE(thread->start());

  auto task = [&statId]() { StatsManager::addValue(statId); };
  constexpr auto qps = 100L;
  thread->addRepeatTask(1 * 1000 / qps, task);

  ::usleep(60 * 1000 * 1000);

  auto actual = StatsManager::readValue("ratetest.rate.60").value();

  ASSERT_LT(std::max(qps, actual) - std::min(qps, actual), 10L)
      << "expected: " << qps << ", actual: " << actual;

  thread->stop();
  thread->wait();
  thread.reset();
}

}  // namespace stats
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
