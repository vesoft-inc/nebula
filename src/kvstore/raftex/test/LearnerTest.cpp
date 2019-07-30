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

TEST(LearnerTest, OneLeaderOneFollowerOneLearnerTest) {
    fs::TempDir walRoot("/tmp/learner_test.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    std::vector<bool> isLearner = {false, false, true};
    // The last one is learner
    setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader, isLearner);

    checkLeadership(copies, leader);

    auto f = leader->sendCommandAsync(test::encodeLearner(allHosts[2]));
    f.wait();

    std::vector<std::string> msgs;
    appendLogs(0, 99, leader, msgs);
    checkConsensus(copies, 0, 99, msgs);

    finishRaft(services, copies, workers, leader);
}

TEST(LearnerTest, OneLeaderTwoLearnerTest) {
    fs::TempDir walRoot("/tmp/learner_test.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    std::vector<bool> isLearner = {false, true, true};
    // Start three services, the first one will be the leader, the left two will be learners.
    setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader, isLearner);

    // The copies[0] is the leader.
    checkLeadership(copies, 0, leader);

    leader->sendCommandAsync(test::encodeLearner(allHosts[1]));
    auto f = leader->sendCommandAsync(test::encodeLearner(allHosts[2]));
    f.wait();

    std::vector<std::string> msgs;
    appendLogs(0, 99, leader, msgs);
    checkConsensus(copies, 0, 99, msgs);

    LOG(INFO) << "Let's kill the two learners, the leader should still work";
    killOneCopy(services, copies, leader, 1);
    killOneCopy(services, copies, leader, 2);

    checkLeadership(copies, 0, leader);

    appendLogs(100, 199, leader, msgs);
    checkConsensus(copies, 100, 199, msgs);

    finishRaft(services, copies, workers, leader);
}

TEST(LearnerTest, CatchUpDataTest) {
    fs::TempDir walRoot("/tmp/catch_up_data.XXXXXX");
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
    appendLogs(0, 99, leader, msgs);
    // Sleep a while to make sure the last log has been committed on followers
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(100, copies[i]->getNumLogs());
    }

    for (int i = 0; i < 100; ++i) {
        for (int j = 0; j < 3; j++) {
            folly::StringPiece msg;
            ASSERT_TRUE(copies[j]->getLogMsg(i, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }

    LOG(INFO) << "Add learner, we need to catch up data!";
    auto f = leader->sendCommandAsync(test::encodeLearner(allHosts[3]));
    f.wait();

    sleep(1);
    auto& learner = copies[3];
    ASSERT_EQ(100, learner->getNumLogs());
    for (int i = 0; i < 100; ++i) {
        folly::StringPiece msg;
        ASSERT_TRUE(learner->getLogMsg(i, msg));
        ASSERT_EQ(msgs[i], msg.toString());
    }

    finishRaft(services, copies, workers, leader);
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


