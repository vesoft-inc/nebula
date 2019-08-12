/* Copyright (c) 2018 vesoft inc. All rights reserved.
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
    leader_->casAsync("TCAS Log Message");
    msgs.emplace_back("CAS Log Message");
    appendLogs(1, 9, leader_, msgs);
    LOG(INFO) << "<===== Finish appending logs";

    checkConsensus(copies_, 0, 9, msgs);
}


TEST_F(LogCASTest, StartWithInvalidCAS) {
    // Append logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    leader_->casAsync("FCAS Log Message");
    appendLogs(0, 9, leader_, msgs);
    LOG(INFO) << "<===== Finish appending logs";

    checkConsensus(copies_, 0, 9, msgs);
}


TEST_F(LogCASTest, ValidCASInMiddle) {
    // Append logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    appendLogs(0, 4, leader_, msgs);

    leader_->casAsync("TCAS Log Message");
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

    leader_->casAsync("FCAS Log Message");

    appendLogs(5, 9, leader_, msgs);
    LOG(INFO) << "<===== Finish appending logs";

    checkConsensus(copies_, 0, 9, msgs);
}


TEST_F(LogCASTest, EndWithValidCAS) {
    // Append logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    appendLogs(0, 7, leader_, msgs);

    leader_->casAsync("TCAS Log Message");
    msgs.emplace_back("CAS Log Message");

    auto fut = leader_->casAsync("TCAS Log Message");
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

    leader_->casAsync("FCAS Log Message");
    auto fut = leader_->casAsync("FCAS Log Message");
    fut.wait();
    LOG(INFO) << "<===== Finish appending logs";

    checkConsensus(copies_, 0, 7, msgs);
}


TEST_F(LogCASTest, AllValidCAS) {
    // Append logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    for (int i = 1; i <= 10; ++i) {
        auto fut = leader_->casAsync("TTest CAS Log");
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
        auto fut = leader_->casAsync("FCAS Log");
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


