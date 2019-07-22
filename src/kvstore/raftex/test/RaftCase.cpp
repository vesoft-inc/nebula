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

    int32_t size = 3;
    fs::TempDir walRoot("/tmp/leader_crash_and_come_back.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;
    std::vector<LogID> lastCommittedLogId;
    std::shared_ptr<test::TestShard> leader;
    setupRaft(size, walRoot, workers, wals, allHosts, services, copies, lastCommittedLogId, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    LOG(INFO) << "=====> Now let's kill the old leader";
    size_t idx = leader->index();
    killOneCopy(services, copies, leader, idx);

    // Wait untill all copies agree on the same leader
    waitUntilLeaderElected(copies, leader);
    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    LOG(INFO) << "=====> Now old leader comes back";
    // Crashed leader reboot
    rebootOneCopy(services, copies, allHosts, idx);

    waitUntilAllHasLeader(copies);
    checkLeadership(copies, leader);

    LOG(INFO) << "=====> Now let's kill leader and a follower, no quorum";
    idx = leader->index();
    killOneCopy(services, copies, leader, idx);
    killOneCopy(services, copies, leader, (idx + 1) % size);
    sleep(2* FLAGS_heartbeat_interval);
    checkNoLeader(copies);

    LOG(INFO) << "=====> Now one of dead copy rejoin, quorum arises";
    rebootOneCopy(services, copies, allHosts, idx);
    waitUntilLeaderElected(copies, leader);

    LOG(INFO) << "=====> Now all copy rejoin, should not disrupt leader";
    rebootOneCopy(services, copies, allHosts, idx);
    sleep(FLAGS_heartbeat_interval);
    waitUntilAllHasLeader(copies);
    checkLeadership(copies, leader);

    finishRaft(services, copies, workers, leader);

    LOG(INFO) << "<===== Done LeaderCrash test";
}

TEST(LeaderElection, ConsensusWhenFollowDisconnect) {
    LOG(INFO) << "=====> Start ConsensusWhenFollowDisconnect test";

    int32_t size = 3;
    fs::TempDir walRoot("/tmp/consensu_when_follower_disconnect.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;
    std::shared_ptr<test::TestShard> leader;
    std::vector<LogID> lastCommittedLogId;
    setupRaft(size, walRoot, workers, wals, allHosts, services, copies, lastCommittedLogId, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);
    LOG(INFO) << "=====> TestShard" << leader->index() << " has become leader";

    std::vector<std::string> msgs;
    appendLogs(0, 0, leader, msgs);
    sleep(FLAGS_heartbeat_interval);
    checkConsensus(copies, 0, 0, msgs);

    // Let's kill one follower
    LOG(INFO) << "=====> Now let's kill one follower";
    size_t idx = (leader->index() + 1) % size;
    killOneCopy(services, copies, leader, idx);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);
    appendLogs(1, 4, leader, msgs);
    sleep(FLAGS_heartbeat_interval);
    checkConsensus(copies, 1, 4, msgs);

    LOG(INFO) << "=====> Now follower comes back";
    // Crashed follower reboot
    rebootOneCopy(services, copies, allHosts, idx);

    waitUntilAllHasLeader(copies);
    checkLeadership(copies, leader);

    appendLogs(5, 6, leader, msgs);
    sleep(FLAGS_heartbeat_interval);
    checkConsensus(copies, 5, 6, msgs);

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
