/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/fs/TempDir.h"
#include "common/network/NetworkUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"

DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace raftex {

class LogCommandTest : public RaftexTestFixture {
 public:
  LogCommandTest() : RaftexTestFixture("log_command_test") {}
};

TEST_F(LogCommandTest, StartWithCommandLog) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  leader_->sendCommandAsync("Command Log Message");
  msgs.emplace_back("Command Log Message");
  appendLogs(1, 9, leader_, msgs, true);
  LOG(INFO) << "<===== Finish appending logs";

  ASSERT_EQ(2, leader_->commitTimes_);
  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCommandTest, CommandInMiddle) {
  FLAGS_raft_heartbeat_interval_secs = 1;
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  appendLogs(0, 4, leader_, msgs);

  leader_->sendCommandAsync("Command Log Message");
  msgs.emplace_back("Command Log Message");

  bool waitLastLog = true;
  appendLogs(6, 9, leader_, msgs, waitLastLog);
  LOG(INFO) << "<===== Finish appending logs";

  ASSERT_EQ(3, leader_->commitTimes_);
  // need to sleep a bit more
  sleep(FLAGS_raft_heartbeat_interval_secs + 1);
  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCommandTest, EndWithCommand) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  appendLogs(0, 8, leader_, msgs);
  auto fut = leader_->sendCommandAsync("Command Log Message");
  msgs.emplace_back("Command Log Message");
  fut.wait();
  LOG(INFO) << "<===== Finish appending logs";

  ASSERT_EQ(2, leader_->commitTimes_);
  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCommandTest, AllCommandLogs) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  for (int i = 1; i <= 10; ++i) {
    auto fut = leader_->sendCommandAsync(folly::stringPrintf("Command log %d", i));
    msgs.emplace_back(folly::stringPrintf("Command log %d", i));
    if (i == 10) {
      fut.wait();
    }
  }
  LOG(INFO) << "<===== Finish appending logs";

  ASSERT_EQ(10, leader_->commitTimes_);
  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCommandTest, MixedLogs) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  // Command, CAS,  Normal, CAS, CAS, Command, Command, Normal, TCAS, Command,
  // Normal, TCAS c        c              c     c      c       c c       c
  std::vector<std::string> msgs;
  leader_->sendCommandAsync("Command log 1");
  msgs.emplace_back("Command log 1");

  kvstore::MergeableAtomicOp op1 = []() {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    auto optStr = test::compareAndSet("TCAS Log Message 2");
    if (optStr) {
      ret.batch = *optStr;
    }
    return ret;
  };
  leader_->atomicOpAsync(std::move(op1));
  msgs.emplace_back("CAS Log Message 2");

  leader_->appendAsync(0, "Normal log Message 3");
  msgs.emplace_back("Normal log Message 3");

  kvstore::MergeableAtomicOp op2 = []() {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    auto optStr = test::compareAndSet("TCAS Log Message 4");
    if (optStr) {
      ret.batch = *optStr;
    }
    return ret;
  };
  leader_->atomicOpAsync(std::move(op2));
  msgs.emplace_back("CAS Log Message 4");

  kvstore::MergeableAtomicOp op3 = []() {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    auto optStr = test::compareAndSet("TCAS Log Message 5");
    if (optStr) {
      ret.batch = *optStr;
    }
    return ret;
  };
  leader_->atomicOpAsync(std::move(op3));
  msgs.emplace_back("CAS Log Message 5");

  leader_->sendCommandAsync("Command log 6");
  msgs.emplace_back("Command log 6");

  leader_->sendCommandAsync("Command log 7");
  msgs.emplace_back("Command log 7");

  leader_->appendAsync(0, "Normal log Message 8");
  msgs.emplace_back("Normal log Message 8");

  kvstore::MergeableAtomicOp op4 = []() {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    auto optStr = test::compareAndSet("FCAS Log Message");
    if (optStr) {
      ret.batch = *optStr;
    }
    return ret;
  };
  leader_->atomicOpAsync(std::move(op4));

  leader_->sendCommandAsync("Command log 9");
  msgs.emplace_back("Command log 9");

  auto f = leader_->appendAsync(0, "Normal log Message 10");
  msgs.emplace_back("Normal log Message 10");

  kvstore::MergeableAtomicOp op5 = []() {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    auto optStr = test::compareAndSet("FCAS Log Message");
    if (optStr) {
      ret.batch = *optStr;
    }
    return ret;
  };
  leader_->atomicOpAsync(std::move(op5));
  // leader_->atomicOpAsync([]() { return test::compareAndSet("FCAS Log Message"); });

  f.wait();
  LOG(INFO) << "<===== Finish appending logs";

  // previous is 8
  ASSERT_EQ(5, leader_->commitTimes_);
  checkConsensus(copies_, 0, 9, msgs);
}

}  // namespace raftex
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
