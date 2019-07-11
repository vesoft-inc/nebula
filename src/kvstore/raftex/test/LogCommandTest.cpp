/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include "fs/TempDir.h"
#include "fs/FileUtils.h"
#include "thread/GenericThreadPool.h"
#include "network/NetworkUtils.h"
#include "kvstore/wal/BufferFlusher.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"

DECLARE_uint32(heartbeat_interval);


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
    for (int i = 2; i <= 10; ++i) {
        msgs.emplace_back(
            folly::stringPrintf("Test Log Message %03d", i));
        auto fut = leader_->appendAsync(0, msgs.back());
        if (i == 10) {
            fut.wait();
        }
    }
    LOG(INFO) << "<===== Finish appending logs";

    ASSERT_EQ(2, leader_->commitTimes_);
    // Sleep a while to make sure the last log has been committed on
    // followers
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies_) {
        ASSERT_EQ(10, c->getNumLogs());
    }

    LogID id = leader_->firstCommittedLogId_;
    for (int i = 0; i < 10; ++i, ++id) {
        for (auto& c : copies_) {
            folly::StringPiece msg;
            ASSERT_TRUE(c->getLogMsg(id, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }
}

TEST_F(LogCommandTest, CommandInMiddle) {
    // Append logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    for (int i = 1; i <= 5; ++i) {
        msgs.emplace_back(
            folly::stringPrintf("Test Log Message %03d", i));
        auto fut = leader_->appendAsync(0, msgs.back());
    }

    leader_->sendCommandAsync("Command Log Message");
    msgs.emplace_back("Command Log Message");

    for (int i = 7; i <= 10; ++i) {
        msgs.emplace_back(
            folly::stringPrintf("Test Log Message %03d", i));
        auto fut = leader_->appendAsync(0, msgs.back());
        if (i == 10) {
            fut.wait();
        }
    }
    LOG(INFO) << "<===== Finish appending logs";

    ASSERT_EQ(2, leader_->commitTimes_);
    // Sleep a while to make sure the last log has been committed on
    // followers
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies_) {
        ASSERT_EQ(10, c->getNumLogs());
    }

    LogID id = leader_->firstCommittedLogId_;
    for (int i = 0; i < 10; ++i, ++id) {
        for (auto& c : copies_) {
            folly::StringPiece msg;
            ASSERT_TRUE(c->getLogMsg(id, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }
}

TEST_F(LogCommandTest, EndWithCommand) {
    // Append logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    for (int i = 1; i <= 9; ++i) {
        msgs.emplace_back(
            folly::stringPrintf("Test Log Message %03d", i));
        leader_->appendAsync(0, msgs.back());
    }
    auto fut = leader_->sendCommandAsync("Command Log Message");
    msgs.emplace_back("Command Log Message");
    fut.wait();
    LOG(INFO) << "<===== Finish appending logs";

    ASSERT_EQ(1, leader_->commitTimes_);
    // Sleep a while to make sure the last log has been committed on
    // followers
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies_) {
        ASSERT_EQ(10, c->getNumLogs());
    }

    LogID id = leader_->firstCommittedLogId_;
    for (int i = 0; i < 10; ++i, ++id) {
        for (auto& c : copies_) {
            folly::StringPiece msg;
            ASSERT_TRUE(c->getLogMsg(id, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }
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

    // Sleep a while to make sure the last log has been committed on
    // followers
    ASSERT_EQ(10, leader_->commitTimes_);
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies_) {
        ASSERT_EQ(10, c->getNumLogs());
    }

    LogID id = leader_->firstCommittedLogId_;
    for (int i = 0; i < 10; ++i, ++id) {
        for (auto& c : copies_) {
            folly::StringPiece msg;
            ASSERT_TRUE(c->getLogMsg(id, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }
}


TEST_F(LogCommandTest, MixedLogs) {
    // Append logs
    LOG(INFO) << "=====> Start appending logs";
    // Command, CAS,  Normal, CAS, CAS, Command, Command, Normal, TCAS, Command, Normal, TCAS
    // c        c              c     c      c       c                      c       c
    std::vector<std::string> msgs;
    leader_->sendCommandAsync("Command log 1");
    msgs.emplace_back("Command log 1");

    leader_->casAsync("TCAS Log Message 2");
    msgs.emplace_back("CAS Log Message 2");

    leader_->appendAsync(0, "Normal log Message 3");
    msgs.emplace_back("Normal log Message 3");

    leader_->casAsync("TCAS Log Message 4");
    msgs.emplace_back("CAS Log Message 4");

    leader_->casAsync("TCAS Log Message 5");
    msgs.emplace_back("CAS Log Message 5");

    leader_->sendCommandAsync("Command log 6");
    msgs.emplace_back("Command log 6");

    leader_->sendCommandAsync("Command log 7");
    msgs.emplace_back("Command log 7");

    leader_->appendAsync(0, "Normal log Message 8");
    msgs.emplace_back("Normal log Message 8");

    leader_->casAsync("FCAS Log Message");

    leader_->sendCommandAsync("Command log 9");
    msgs.emplace_back("Command log 9");

    auto f = leader_->appendAsync(0, "Normal log Message 10");
    msgs.emplace_back("Normal log Message 10");

    leader_->casAsync("FCAS Log Message");

    f.wait();
    LOG(INFO) << "<===== Finish appending logs";

    // Sleep a while to make sure the last log has been committed on
    // followers
    ASSERT_EQ(8, leader_->commitTimes_);
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies_) {
        ASSERT_EQ(10, c->getNumLogs());
    }

    LogID id = leader_->firstCommittedLogId_;
    for (int i = 0; i < 10; ++i, ++id) {
        for (auto& c : copies_) {
            folly::StringPiece msg;
            ASSERT_TRUE(c->getLogMsg(id, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }
}


}  // namespace raftex
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    // `flusher' is extern-declared in RaftexTestBase.h, defined in RaftexTestBase.cpp
    using nebula::raftex::flusher;
    flusher = std::make_unique<nebula::wal::BufferFlusher>();

    return RUN_ALL_TESTS();
}


