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
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"

DECLARE_uint32(raft_heartbeat_interval_secs);


namespace nebula {
namespace raftex {

TEST(MemberChangeTest, AddRemovePeerTest) {
    fs::TempDir walRoot("/tmp/member_change.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<network::InetAddress> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    std::vector<bool> isLearner = {false, false, false, true};
    setupRaft(4, walRoot, workers, wals, allHosts, services, copies, leader, isLearner);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    CHECK_EQ(2, leader->hosts_.size());

    {
        auto f = leader->sendCommandAsync(test::encodeAddPeer(allHosts[3]));
        f.wait();

        for (auto& c : copies) {
            CHECK_EQ(3, c->hosts_.size());
        }
    }
    std::vector<std::string> msgs;
    LogID id = -1;
    appendLogs(0, 99, leader, msgs, id);
    // Sleep a while to make sure the last log has been committed on
    // followers
    sleep(FLAGS_raft_heartbeat_interval_secs);

    checkConsensus(copies, 0, 99, msgs);

    {
        LOG(INFO) << "Add the same peer again!";
        auto f = leader->sendCommandAsync(test::encodeAddPeer(allHosts[3]));
        f.wait();
        // sleep a while to ensure the learner receive the command.
        sleep(1);
        for (auto& c : copies) {
            CHECK_EQ(3, c->hosts_.size());
        }
    }
    {
        LOG(INFO) << "Remove the peer added!";
        auto f = leader->sendCommandAsync(test::encodeRemovePeer(allHosts[3]));
        f.wait();
        // sleep a while to ensure the learner receive the command.
        sleep(1);
        for (size_t i = 0; i < copies.size() - 1; i++) {
            CHECK_EQ(2, copies[i]->hosts_.size());
        }
//        CHECK(copies[3]->isStopped());
    }
    finishRaft(services, copies, workers, leader);
}

TEST(MemberChangeTest, RemoveLeaderTest) {
    fs::TempDir walRoot("/tmp/member_change.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<network::InetAddress> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    std::vector<bool> isLearner = {false, false, false, false};
    setupRaft(4, walRoot, workers, wals, allHosts, services, copies, leader, isLearner);

    // Check all hosts agree on the same leader
    auto leaderIndex = checkLeadership(copies, leader);

    CHECK_EQ(3, leader->hosts_.size());

    {
        LOG(INFO) << "Send remove peer request, remove " << allHosts[leaderIndex];
        leader.reset();
        auto f = copies[leaderIndex]->sendCommandAsync(
                        test::encodeRemovePeer(allHosts[leaderIndex]));
        f.wait();
        copies[leaderIndex]->stop();
//        CHECK(copies[leaderIndex]->isStopped());
        for (size_t i = 0; i < copies.size(); i++) {
            if (static_cast<int>(i) != leaderIndex) {
                CHECK_EQ(2, copies[i]->hosts_.size());
            }
        }
    }

    waitUntilLeaderElected(copies, leader);

    auto nLeaderIndex = checkLeadership(copies, leader);
    CHECK(leaderIndex != nLeaderIndex);

    std::vector<std::string> msgs;
    LogID id = -1;
    appendLogs(0, 99, leader, msgs, id);
    // Sleep a while to make sure the last log has been committed on
    // followers
    sleep(FLAGS_raft_heartbeat_interval_secs);

    checkConsensus(copies, 0, 99, msgs);

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


