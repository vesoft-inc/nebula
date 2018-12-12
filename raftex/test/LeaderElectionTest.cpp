/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include "fs/TempDir.h"
#include "fs/FileUtils.h"
#include "thread/GenericThreadPool.h"
#include "network/NetworkUtils.h"
#include "raftex/BufferFlusher.h"
#include "raftex/RaftexService.h"
#include "raftex/test/RaftexTestBase.h"
#include "raftex/test/TestShard.h"

namespace vesoft {
namespace raftex {

TEST(LeaderElection, ElectionAfterBoot) {
    LOG(INFO) << "=====> Start ElectionAfterBoot test";
    fs::TempDir walRoot("/tmp/election_after_boot.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    finishRaft(services, copies, workers, leader);

    LOG(INFO) << "<===== Done ElectionAfterBoot test";
}


TEST(LeaderElection, LeaderCrash) {
    LOG(INFO) << "=====> Start LeaderCrash test";

    fs::TempDir walRoot("/tmp/leader_election.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    // Let's kill the old leader
    LOG(INFO) << "=====> Now let's kill the old leader";

    size_t idx = leader->index();
    copies[idx].reset();
    services[idx]->removePartition(leader);
    {
        std::lock_guard<std::mutex> lock(leaderMutex);
        leader.reset();
    }

    // Add a new copy
    copies.emplace_back(std::make_shared<test::TestShard>(
        copies.size(),
        services[idx],
        1,  // Shard ID
        allHosts[idx],
        wals[3],
        flusher.get(),
        services[idx]->getIOThreadPool(),
        workers,
        std::bind(&onLeadershipLost,
                  std::ref(copies),
                  std::ref(leader),
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3),
        std::bind(&onLeaderElected,
                  std::ref(copies),
                  std::ref(leader),
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3)));
    services[idx]->addPartition(copies.back());
    copies.back()->start(getPeers(allHosts, allHosts[idx]));

    // Wait untill all copies agree on the same leader
    waitUntilLeaderElected(copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    finishRaft(services, copies, workers, leader);

    LOG(INFO) << "<===== Done LeaderCrash test";
}

}  // namespace raftex
}  // namespace vesoft


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    using namespace vesoft::raftex;
    flusher = std::make_unique<BufferFlusher>();

    return RUN_ALL_TESTS();
}


