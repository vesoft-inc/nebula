/* Copyright (c) 2018 vesoft inc. All rights reserved.
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

class LogCASTest : public RaftexTestFixture {
 public:
  LogCASTest() : RaftexTestFixture("log_cas_test") {}
};

TEST_F(LogCASTest, StartWithValidCAS) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  leader_->atomicOpAsync([] {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    ret.batch = "CAS Log Message";
    return ret;
  });
  msgs.emplace_back("CAS Log Message");
  appendLogs(1, 9, leader_, msgs);
  LOG(INFO) << "<===== Finish appending logs";

  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCASTest, StartWithInvalidCAS) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  leader_->atomicOpAsync([] {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    return ret;
  });
  appendLogs(0, 9, leader_, msgs);
  LOG(INFO) << "<===== Finish appending logs";

  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCASTest, ValidCASInMiddle) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  appendLogs(0, 4, leader_, msgs);

  leader_->atomicOpAsync([] {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    ret.batch = "CAS Log Message";
    return ret;
  });
  msgs.emplace_back("CAS Log Message");

  appendLogs(6, 9, leader_, msgs);
  LOG(INFO) << "<===== Finish appending logs";

  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCASTest, InvalidCASInMiddle) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  appendLogs(0, 4, leader_, msgs);

  leader_->atomicOpAsync([] {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    return ret;
  });

  appendLogs(5, 9, leader_, msgs);
  LOG(INFO) << "<===== Finish appending logs";

  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCASTest, EndWithValidCAS) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  appendLogs(0, 7, leader_, msgs);

  // leader_->atomicOpAsync([]() { return test::compareAndSet("TCAS Log Message"); });
  leader_->atomicOpAsync([] {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    ret.batch = "CAS Log Message";
    return ret;
  });
  msgs.emplace_back("CAS Log Message");

  auto fut = leader_->atomicOpAsync([] {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    ret.batch = "CAS Log Message";
    return ret;
  });
  msgs.emplace_back("CAS Log Message");
  fut.wait();
  LOG(INFO) << "<===== Finish appending logs";

  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCASTest, EndWithInvalidCAS) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  appendLogs(0, 7, leader_, msgs);

  auto fut = leader_->atomicOpAsync([] {
    kvstore::MergeableAtomicOpResult ret;
    ret.code = ::nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    return ret;
  });

  fut.wait();
  LOG(INFO) << "<===== Finish appending logs";

  checkConsensus(copies_, 0, 7, msgs);
}

TEST_F(LogCASTest, AllValidCAS) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  for (int i = 1; i <= 10; ++i) {
    auto fut = leader_->atomicOpAsync([] {
      kvstore::MergeableAtomicOpResult ret;
      ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
      ret.batch = "Test CAS Log";
      return ret;
    });
    msgs.emplace_back("Test CAS Log");
    if (i == 10) {
      fut.wait();
    }
  }
  LOG(INFO) << "<===== Finish appending logs";

  checkConsensus(copies_, 0, 9, msgs);
}

TEST_F(LogCASTest, AllInvalidCAS) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  for (int i = 1; i <= 10; ++i) {
    // auto fut = leader_->atomicOpAsync([]() { return test::compareAndSet("FCAS Log"); });

    auto fut = leader_->atomicOpAsync([] {
      kvstore::MergeableAtomicOpResult ret;
      ret.code = ::nebula::cpp2::ErrorCode::E_RPC_FAILURE;
      return ret;
    });

    if (i == 10) {
      fut.wait();
    }
  }
  LOG(INFO) << "<===== Finish appending logs";

  // Sleep a while to make sure the last log has been committed on followers
  sleep(FLAGS_raft_heartbeat_interval_secs);

  // Check every copy
  for (auto& c : copies_) {
    ASSERT_EQ(0, c->getNumLogs());
  }
}

TEST_F(LogCASTest, OnlyOneCasSucceed) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  for (int i = 1; i <= 10; ++i) {
    std::string log;
    if (i == 1) {
      log = "TCAS Log " + std::to_string(i);
      msgs.emplace_back(log.substr(1));
    } else {
      log = "FCAS Log " + std::to_string(i);
    }
    auto fut = leader_->atomicOpAsync([=] {
      kvstore::MergeableAtomicOpResult ret;
      ret.code = ::nebula::cpp2::ErrorCode::E_RPC_FAILURE;
      if (i == 1) {
        ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
        ret.batch = msgs.back();
      }
      return ret;
    });
    if (i == 10) {
      fut.wait();
    }
  }
  LOG(INFO) << "<===== Finish appending logs";

  // Sleep a while to make sure the last log has been committed on followers
  sleep(FLAGS_raft_heartbeat_interval_secs);

  // Check every copy
  for (auto& c : copies_) {
    ASSERT_EQ(1, c->getNumLogs());
  }
  checkConsensus(copies_, 0, 0, msgs);
}

TEST_F(LogCASTest, ZipCasTest) {
  // Append logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  for (int i = 1; i <= 10; ++i) {
    std::string log;
    if (i % 2) {
      log = "TCAS Log " + std::to_string(i);
      msgs.emplace_back(log.substr(1));
    } else {
      log = "FCAS Log " + std::to_string(i);
    }
    auto fut = leader_->atomicOpAsync([=] {
      kvstore::MergeableAtomicOpResult ret;
      ret.code = ::nebula::cpp2::ErrorCode::E_RPC_FAILURE;
      if (i % 2) {
        ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
        ret.batch = msgs.back();
      }
      return ret;
    });
    if (i == 10) {
      fut.wait();
    }
  }
  LOG(INFO) << "<===== Finish appending logs";

  // Sleep a while to make sure the last log has been committed on followers
  sleep(FLAGS_raft_heartbeat_interval_secs);

  // Check every copy
  for (auto& c : copies_) {
    ASSERT_EQ(5, c->getNumLogs());
  }
  checkConsensus(copies_, 0, 4, msgs);
}

TEST_F(LogCASTest, EmptyTest) {
  {
    LOG(INFO) << "return empty string for atomic operation!";
    folly::Baton<> baton;
    leader_
        ->atomicOpAsync([]() mutable {
          kvstore::MergeableAtomicOpResult ret;
          ret.code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
          // ret.batch = log;
          return ret;
        })
        .thenValue([&baton](nebula::cpp2::ErrorCode res) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, res);
          baton.post();
        });
    baton.wait();
  }
  {
    LOG(INFO) << "return none string for atomic operation!";
    folly::Baton<> baton;
    // leader_->atomicOpAsync([log = std::move(log)]() mutable { return folly::none; })
    leader_
        ->atomicOpAsync([]() mutable {
          kvstore::MergeableAtomicOpResult ret;
          ret.code = ::nebula::cpp2::ErrorCode::E_RAFT_ATOMIC_OP_FAILED;
          // ret.batch = log;
          return ret;
        })
        .thenValue([&baton](nebula::cpp2::ErrorCode res) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::E_RAFT_ATOMIC_OP_FAILED, res);
          baton.post();
        });
    baton.wait();
  }
}

}  // namespace raftex
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
