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
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"

DECLARE_uint32(heartbeat_interval);


namespace nebula {
namespace raftex {


TEST(LeaderTransferTest, SimpleTest) {
    fs::TempDir walRoot("/tmp/leader_transfer_test.simple_test.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    auto index = checkLeadership(copies, leader);

    auto nLeaderIndex = (index + 1) % 3;
    auto f = leader->sendCommandAsync(test::encodeTransferLeader(allHosts[nLeaderIndex]));
    f.wait();

    leader.reset();
    waitUntilLeaderElected(copies, leader);

    checkLeadership(copies, nLeaderIndex, leader);

    // Append 100 logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    appendLogs(0, 99, leader, msgs);
    LOG(INFO) << "<===== Finish appending logs";

    checkConsensus(copies, 0, 99, msgs);
    finishRaft(services, copies, workers, leader);
}

TEST(LeaderTransferTest, ChangeLeaderServalTimesTest) {
    fs::TempDir walRoot("/tmp/leader_transfer_test.simple_test.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(3, walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    auto nLeaderIndex = checkLeadership(copies, leader);
    int32_t times = 0;
    while (++times <= 10) {
        auto leaderIndex = nLeaderIndex;
        nLeaderIndex = (nLeaderIndex + 1) % 3;
        LOG(INFO) << times << " ===== Let's transfer the leader from " << allHosts[leaderIndex]
                           << " to " << allHosts[nLeaderIndex];
        leader.reset();
        auto f = copies[leaderIndex]->sendCommandAsync(
                                test::encodeTransferLeader(allHosts[nLeaderIndex]));
        f.wait();
        waitUntilLeaderElected(copies, leader);
        checkLeadership(copies, nLeaderIndex, leader);
    }

    // Append 100 logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    appendLogs(0, 99, leader, msgs);
    LOG(INFO) << "<===== Finish appending logs";
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


