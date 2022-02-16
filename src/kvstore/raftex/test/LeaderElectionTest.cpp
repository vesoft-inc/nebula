/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <stddef.h>           // for size_t

#include <ext/alloc_traits.h>  // for __alloc_traits<>::va...
#include <functional>          // for ref, bind, _1, _2, _3
#include <memory>              // for shared_ptr, __shared...
#include <mutex>               // for lock_guard, mutex
#include <ostream>             // for operator<<
#include <string>              // for string, basic_string
#include <vector>              // for vector

#include "common/base/Logging.h"                 // for LOG, LogMessage, _LO...
#include "common/datatypes/HostAddr.h"           // for HostAddr
#include "common/fs/TempDir.h"                   // for TempDir
#include "kvstore/raftex/RaftexService.h"        // for RaftexService
#include "kvstore/raftex/test/RaftexTestBase.h"  // for checkLeadership, fin...
#include "kvstore/raftex/test/TestShard.h"       // for TestShard

namespace nebula {
namespace thread {
class GenericThreadPool;

class GenericThreadPool;
}  // namespace thread

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
  copies.emplace_back(std::make_shared<test::TestShard>(copies.size(),
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

  // Wait until all copies agree on the same leader
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
