/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/fs/FileUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"
#include <gtest/gtest.h>
#include <folly/String.h>

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
                    auto fut = leader->appendAsync(
                        0, folly::stringPrintf("Log %03d for t%d", j, i));
                    if (fut.isReady() &&
                        fut.value() == AppendLogResult::E_BUFFER_OVERFLOW) {
                        LOG(FATAL) << "Should not reach here";
                    } else if (j == numLogs) {
                        // Only wait on the last log messaage
                        ASSERT_EQ(AppendLogResult::SUCCEEDED, std::move(fut).get());
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
