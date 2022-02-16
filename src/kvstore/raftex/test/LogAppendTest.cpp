/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Range.h>                           // for Range
#include <folly/String.h>                          // for stringPrintf
#include <folly/futures/Future.h>                  // for Future::get, Futur...
#include <folly/futures/Future.h>                  // for Future
#include <folly/init/Init.h>                       // for init
#include <folly/io/async/ScopedEventBaseThread.h>  // for StringPiece
#include <gflags/gflags_declare.h>                 // for DECLARE_uint32
#include <glog/logging.h>                          // for INFO
#include <gtest/gtest.h>                           // for TestPartResult
#include <gtest/gtest.h>                           // for Message
#include <gtest/gtest.h>                           // for TestPartResult
#include <unistd.h>                                // for sleep

#include <memory>       // for shared_ptr, allocator
#include <ostream>      // for operator<<
#include <string>       // for string, basic_string
#include <thread>       // for thread
#include <type_traits>  // for remove_reference<>...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"                 // for LOG, SetStderrLogging
#include "common/datatypes/HostAddr.h"           // for HostAddr
#include "common/fs/TempDir.h"                   // for TempDir
#include "interface/gen-cpp2/common_types.h"     // for ErrorCode, ErrorCo...
#include "kvstore/raftex/test/RaftexTestBase.h"  // for checkLeadership
#include "kvstore/raftex/test/TestShard.h"       // for TestShard

namespace nebula {
namespace thread {
class GenericThreadPool;
}  // namespace thread

namespace raftex {
class RaftexService;

class RaftexService;
}  // namespace raftex
namespace thread {
class GenericThreadPool;
}  // namespace thread
}  // namespace nebula

DECLARE_uint32(raft_heartbeat_interval_secs);
DECLARE_uint32(max_batch_size);

namespace nebula {
namespace raftex {

TEST(LogAppend, SimpleAppendWithOneCopy) {
  fs::TempDir walRoot("/tmp/simple_append_with_one_copy.XXXXXX");
  std::shared_ptr<thread::GenericThreadPool> workers;
  std::vector<std::string> wals;
  std::vector<HostAddr> allHosts;
  std::vector<std::shared_ptr<RaftexService>> services;
  std::vector<std::shared_ptr<test::TestShard>> copies;

  std::shared_ptr<test::TestShard> leader;
  setupRaft(1, walRoot, workers, wals, allHosts, services, copies, leader);

  // Check all hosts agree on the same leader
  checkLeadership(copies, leader);

  std::vector<std::string> msgs;
  appendLogs(0, 99, leader, msgs);
  checkConsensus(copies, 0, 99, msgs);

  finishRaft(services, copies, workers, leader);
}

TEST(LogAppend, SimpleAppendWithThreeCopies) {
  fs::TempDir walRoot("/tmp/simple_append_with_three_copies.XXXXXX");
  std::shared_ptr<thread::GenericThreadPool> workers;
  std::vector<std::string> wals;
  std::vector<HostAddr> allHosts;
  std::vector<std::shared_ptr<RaftexService>> services;
  std::vector<std::shared_ptr<test::TestShard>> copies;

  std::shared_ptr<test::TestShard> leader;
  setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader);

  // Check all hosts agree on the same leader
  checkLeadership(copies, leader);

  std::vector<std::string> msgs;
  appendLogs(0, 99, leader, msgs);
  checkConsensus(copies, 0, 99, msgs);

  finishRaft(services, copies, workers, leader);
}

TEST(LogAppend, MultiThreadAppend) {
  fs::TempDir walRoot("/tmp/multi_thread_append.XXXXXX");
  std::shared_ptr<thread::GenericThreadPool> workers;
  std::vector<std::string> wals;
  std::vector<HostAddr> allHosts;
  std::vector<std::shared_ptr<RaftexService>> services;
  std::vector<std::shared_ptr<test::TestShard>> copies;

  std::shared_ptr<test::TestShard> leader;
  setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader);

  // Check all hosts agree on the same leader
  checkLeadership(copies, leader);

  // Create 4 threads, each appends 100 logs
  LOG(INFO) << "=====> Start multi-thread appending logs";
  const int numThreads = 4;
  const int numLogs = 100;
  FLAGS_max_batch_size = numThreads * numLogs + 1;
  std::vector<std::thread> threads;
  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back(std::thread([i, leader] {
      for (int j = 1; j <= numLogs; ++j) {
        do {
          auto fut = leader->appendAsync(0, folly::stringPrintf("Log %03d for t%d", j, i));
          if (fut.isReady() && fut.value() == nebula::cpp2::ErrorCode::E_RAFT_BUFFER_OVERFLOW) {
            LOG(FATAL) << "Should not reach here";
          } else if (j == numLogs) {
            // Only wait on the last log message
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, std::move(fut).get());
          }
          break;
        } while (true);
      }
    }));
  }

  // Wait for all threads to finish
  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << "<===== Finish multi-thread appending logs";

  // Sleep a while to make sure the last log has been committed on
  // followers
  sleep(FLAGS_raft_heartbeat_interval_secs);

  // Check every copy
  for (auto& c : copies) {
    ASSERT_EQ(numThreads * numLogs, c->getNumLogs());
  }

  for (int i = 0; i < numThreads * numLogs; ++i) {
    folly::StringPiece msg;
    ASSERT_TRUE(leader->getLogMsg(i, msg));
    for (auto& c : copies) {
      if (c != leader) {
        folly::StringPiece log;
        ASSERT_TRUE(c->getLogMsg(i, log));
        ASSERT_EQ(msg, log);
      }
    }
  }

  finishRaft(services, copies, workers, leader);
}

}  // namespace raftex
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
