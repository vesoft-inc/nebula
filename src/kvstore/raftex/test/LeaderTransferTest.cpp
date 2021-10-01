/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/fs/TempDir.h"
#include "common/network/NetworkUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"

DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace raftex {

TEST(LeaderTransferTest, SimpleTest) {
  FLAGS_raft_heartbeat_interval_secs = 1;
  fs::TempDir walRoot("/tmp/leader_transfer_test.simple_test.XXXXXX");
  std::shared_ptr<thread::GenericThreadPool> workers;
  std::shared_ptr<folly::IOThreadPoolExecutor> clientPool;
  std::vector<std::string> wals;
  std::vector<HostAddr> allHosts;
  std::vector<std::shared_ptr<RaftexService>> services;
  std::vector<std::shared_ptr<test::TestShard>> copies;
  std::shared_ptr<test::TestShard> leader;

  SCOPE_EXIT {
    finishRaft(services, copies, workers, clientPool, leader);
  };

  setupRaft(3, walRoot, workers, clientPool, wals, allHosts, services, copies, leader);

  // Check all hosts agree on the same leader
  ASSERT_TRUE(checkLeadership(copies, leader));
  auto index = leader->index();

  auto nLeaderIndex = (index + 1) % 3;
  auto f = leader->sendCommandAsync(test::encodeTransferLeader(allHosts[nLeaderIndex]));
  leader.reset();
  f.wait();

  waitUntilLeaderElected(copies, leader);

  ASSERT_NE(index, leader->index());
//  ASSERT_TRUE(checkLeadership(copies, nLeaderIndex, leader));

  // Append 100 logs
  LOG(INFO) << "=====> Start appending logs";
  std::vector<std::string> msgs;
  appendLogs(0, 99, leader, msgs, true);
  LOG(INFO) << "<===== Finish appending logs";

  ASSERT_TRUE(checkConsensus(copies, 0, 99, msgs));
}

TEST(LeaderTransferTest, DISABLED_ChangeLeaderServalTimesTest) {
  fs::TempDir walRoot("/tmp/leader_transfer_test.simple_test.XXXXXX");
  std::shared_ptr<thread::GenericThreadPool> workers;
  std::shared_ptr<folly::IOThreadPoolExecutor> clientPool;
  std::vector<std::string> wals;
  std::vector<HostAddr> allHosts;
  std::vector<std::shared_ptr<RaftexService>> services;
  std::vector<std::shared_ptr<test::TestShard>> copies;
  std::shared_ptr<test::TestShard> leader;

  SCOPE_EXIT {
    finishRaft(services, copies, workers, clientPool, leader);
  };

  setupRaft(3, walRoot, workers, clientPool, wals, allHosts, services, copies, leader);

  // Check all hosts agree on the same leader
  ASSERT_TRUE(checkLeadership(copies, leader));
  auto nLeaderIndex = leader->index();
  int32_t times = 0;
  int32_t logIndex = 0;
  std::vector<std::string> msgs;
  while (++times <= 10) {
    auto leaderIndex = nLeaderIndex;
    nLeaderIndex = (nLeaderIndex + 1) % 3;
    LOG(INFO) << times << " ===== Let's transfer the leader from " << allHosts[leaderIndex]
              << " to " << allHosts[nLeaderIndex];
    leader.reset();
    auto f =
        copies[leaderIndex]->sendCommandAsync(test::encodeTransferLeader(allHosts[nLeaderIndex]));
    f.wait();
    waitUntilLeaderElected(copies, leader);
    ASSERT_TRUE(checkLeadership(copies, nLeaderIndex, leader));
    LOG(INFO) << "=====> Start appending logs";
    appendLogs(logIndex, logIndex, leader, msgs, true);
    ASSERT_TRUE(checkConsensus(copies, 0, logIndex, msgs));
    LOG(INFO) << "<===== Finish appending logs";
    logIndex++;
  }
}

}  // namespace raftex
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
