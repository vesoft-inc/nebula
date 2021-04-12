/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_int32(wal_buffer_num);
DECLARE_int32(raft_rpc_timeout_ms);

namespace nebula {
namespace raftex {

TEST(SnapshotTest, LearnerCatchUpDataTest) {
    fs::TempDir walRoot("/tmp/catch_up_data.XXXXXX");
    FLAGS_wal_file_size = 1024;
    FLAGS_wal_buffer_size = 512;
    FLAGS_raft_rpc_timeout_ms = 2000;
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    std::vector<bool> isLearner = {false, false, false, true};
    setupRaft(4, walRoot, workers, wals, allHosts, services, copies, leader, isLearner);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    std::vector<std::string> msgs;
    for (int i = 0; i < 10; i++) {
        appendLogs(i * 100, i * 100 + 99, leader, msgs, true);
    }
    // Sleep a while to make sure the last log has been committed on followers
    sleep(FLAGS_raft_heartbeat_interval_secs);

    // Check every copy
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(1000, copies[i]->getNumLogs());
    }

    for (int i = 0; i < 1000; ++i) {
        for (int j = 0; j < 3; j++) {
            folly::StringPiece msg;
            ASSERT_TRUE(copies[j]->getLogMsg(i, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }
    // wait for the wal to be cleaned
    FLAGS_wal_ttl = 1;
    sleep(FLAGS_wal_ttl + 3);
    FLAGS_wal_ttl = 60;
    LOG(INFO) << "Add learner, we need to catch up data!";
    auto f = leader->sendCommandAsync(test::encodeLearner(allHosts[3]));
    f.wait();

    LOG(INFO) << "Let's continue to write some logs";
    for (int i = 10; i < 20; i++) {
        appendLogs(i * 100, i * 100 + 99, leader, msgs, true);
    }
    sleep(FLAGS_raft_heartbeat_interval_secs);

    auto& learner = copies[3];
    ASSERT_EQ(2000, learner->getNumLogs());
    for (int i = 0; i < 2000; ++i) {
        folly::StringPiece msg;
        ASSERT_TRUE(learner->getLogMsg(i, msg));
        ASSERT_EQ(msgs[i], msg.toString());
    }

    LOG(INFO) << "Finished UT";
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


