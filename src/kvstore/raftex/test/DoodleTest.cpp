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

DECLARE_uint32(heartbeat_interval);

namespace nebula {
namespace raftex {

TEST(RaftTest, LeaderCrashAndComeBack) {
    LOG(INFO) << "=====> Start LeaderCrash test";

    fs::TempDir walRoot("/tmp/leader_crash_and_come_back.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;
    std::shared_ptr<test::TestShard> leader;
    setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    // Let's kill the old leader
    LOG(INFO) << "=====> Now let's kill the old leader";
    size_t idx = leader->index();
    copies[idx]->isRunning_ = false;
    services[idx]->removePartition(copies[idx]);
    {
        std::lock_guard<std::mutex> lock(leaderMutex);
        leader.reset();
    }

    // Wait untill all copies agree on the same leader
    waitUntilLeaderElected(copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    LOG(INFO) << "=====> Now old leader comes back";
    // Crashed leader reboot
    services[idx]->addPartition(copies[idx]);
    copies[idx]->start(getPeers(allHosts, allHosts[idx]));
    copies[idx]->isRunning_ = true;

    waitUntilAllHasLeader(copies);
    checkLeadership(copies, leader);

    finishRaft(services, copies, workers, leader);

    LOG(INFO) << "<===== Done LeaderCrash test";
}

TEST(LeaderElection, ConsensusWhenFollowDisconnect) {
    LOG(INFO) << "=====> Start ConsensusWhenFollowDisconnect test";

    int size = 3;
    fs::TempDir walRoot("/tmp/consensu_when_follower_disconnect.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;
    std::shared_ptr<test::TestShard> leader;
    setupRaft(size, walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);
    LOG(INFO) << "=====> TestShard" << leader->index() << " has become leader";

    std::vector<std::string> msgs;
    LogID id = -1;
    appendLogs(0, 0, leader, msgs, id);
    checkConsensus(copies, leader, 0, 0, msgs);

    // Let's kill one follower
    LOG(INFO) << "=====> Now let's kill one follower";
    size_t idx = (leader->index() + 1) % size;
    copies[idx]->isRunning_ = false;
    services[idx]->removePartition(copies[idx]);

    appendLogs(1, 2, leader, msgs, id);
    checkConsensus(copies, leader, 1, 2, msgs);
    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);
    appendLogs(3, 4, leader, msgs, id);
    checkConsensus(copies, leader, 3, 4, msgs);

    LOG(INFO) << "=====> Now follower comes back";
    // Crashed follower reboot
    services[idx]->addPartition(copies[idx]);
    copies[idx]->start(getPeers(allHosts, allHosts[idx]));
    copies[idx]->isRunning_ = true;

    waitUntilAllHasLeader(copies);
    checkLeadership(copies, leader);

    appendLogs(5, 6, leader, msgs, id);
    checkConsensus(copies, leader, 5, 6, msgs);

    finishRaft(services, copies, workers, leader);

    LOG(INFO) << "<===== Done ConsensusWhenFollowDisconnect test";
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
