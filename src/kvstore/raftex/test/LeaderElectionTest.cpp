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
#include <gtest/gtest.h>
#include <folly/String.h>
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"

namespace nebula {
namespace raftex {

TEST(LeaderElection, ElectionWithThreeCopies) {
    LOG(INFO) << "=====> Start ElectionWithThreeCopies test";
    fs::TempDir walRoot("/tmp/election_with_three_copies.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    finishRaft(services, copies, workers, leader);

    LOG(INFO) << "<===== Done ElectionWithThreeCopies test";
}


TEST(LeaderElection, ElectionWithOneCopy) {
    LOG(INFO) << "=====> Start ElectionWithOneCopy test";
    fs::TempDir walRoot("/tmp/election_with_one_copy.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(1, walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    finishRaft(services, copies, workers, leader);

    LOG(INFO) << "<===== Done ElectionWithOneCopy test";
}


TEST(LeaderElection, LeaderCrash) {
    LOG(INFO) << "=====> Start LeaderCrash test";

    fs::TempDir walRoot("/tmp/leader_crash.XXXXXX");
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
        services[idx]->getIOThreadPool(),
        workers,
        services[idx]->getThreadManager(),
        nullptr,
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
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}


