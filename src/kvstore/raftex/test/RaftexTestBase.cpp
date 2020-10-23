/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/thrift/ThriftClientManager.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/TestShard.h"

DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace raftex {

using network::NetworkUtils;
using fs::FileUtils;

std::mutex leaderMutex;
std::condition_variable leaderCV;


std::vector<HostAddr> getPeers(const std::vector<HostAddr>& all,
                               const HostAddr& self,
                               std::vector<bool> isLearner) {
    if (isLearner.empty()) {
        isLearner.resize(all.size(), false);
    }
    std::vector<HostAddr> peers;
    size_t index = 0;
    for (const auto& host : all) {
        if (host != self && !isLearner[index]) {
            VLOG(2) << "Adding host "
                    << host.host
                    << ":" << host.port;
            peers.emplace_back(host);
        }
        index++;
    }

    return peers;
}


void onLeaderElected(
        std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader,
        size_t idx,
        const char* idStr,
        TermID term) {
    LOG(INFO) << idStr << "I'm elected as the leader for the term "
              << term;

    {
        std::lock_guard<std::mutex> g(leaderMutex);
        leader = copies.at(idx);
    }
    leaderCV.notify_one();
}


void onLeadershipLost(
        std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader,
        size_t idx,
        const char* idStr,
        TermID term) {
    LOG(INFO) << idStr << "Term " << term
              << " is passed. I'm not the leader any more";

    {
        std::lock_guard<std::mutex> g(leaderMutex);
        if (leader == copies.at(idx)) {
            leader.reset();
        }
    }
}


void waitUntilLeaderElected(
        const std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader,
        std::vector<bool> isLearner) {
    if (isLearner.empty()) {
        isLearner.resize(copies.size(), false);
    }
    auto checkLeader = [&] () -> bool {
        int32_t index = 0;
        for (auto& c : copies) {
            // if a copy in abnormal state, its leader is (0, 0)
            VLOG(3) << c->address() << " , leader address "
                    << c->leader() << ", elected one "
                    << leader->address();
            if (!isLearner[index] && c != nullptr && leader != c && c->isRunning()) {
                if (leader->address() != c->leader()) {
                    return false;
                }
            }
            index++;
        }
        return true;
    };
    while (true) {
        bool sameLeader = false;
        {
            std::unique_lock<std::mutex> lock(leaderMutex);
            leaderCV.wait(lock, [&leader] {
                if (!leader) {
                    return false;
                } else {
                    return true;
                }
            });

            // Sleep some time to wait until resp of heartbeat has come back when elected as leader
            usleep(50000);
            sameLeader = checkLeader();
        }
        if (sameLeader) {
            // Sleep a while in case concurrent leader election occurs
            sleep(1);
            if (checkLeader()) {
                break;
            }
        }

        // Wait for one second
        sleep(1);
    }
}

void waitUntilAllHasLeader(const std::vector<std::shared_ptr<test::TestShard>>& copies) {
    while (true) {
        bool allHaveLeader = true;
        for (auto& c : copies) {
            if (c != nullptr && c->isRunning()) {
                if (c->leader().host == "" && c->leader().port == 0) {
                    allHaveLeader = false;
                    break;
                }
            }
        }
        if (allHaveLeader) {
            return;
        }
        // Wait for one second
        sleep(1);
    }
}

void setupRaft(
        int32_t numCopies,
        fs::TempDir& walRoot,
        std::shared_ptr<thread::GenericThreadPool>& workers,
        std::vector<std::string>& wals,
        std::vector<HostAddr>& allHosts,
        std::vector<std::shared_ptr<RaftexService>>& services,
        std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader,
        std::vector<bool> isLearner) {
    std::string ipStr("127.0.0.1");

    workers = std::make_shared<thread::GenericThreadPool>();
    workers->start(4);

    // Set up WAL folders (Create one extra for leader crash test)
    for (int i = 0; i < numCopies + 1; ++i) {
        wals.emplace_back(
            folly::stringPrintf("%s/copy%d",
                                walRoot.path(),
                                i + 1));
        CHECK(FileUtils::makeDir(wals.back()));
    }

    // Set up services
    for (int i = 0; i < numCopies; ++i) {
        services.emplace_back(RaftexService::createService(nullptr, nullptr));
        if (!services.back()->start())
            return;
        uint16_t port = services.back()->getServerPort();
        allHosts.emplace_back(ipStr, port);
    }

    if (isLearner.empty()) {
        isLearner.resize(allHosts.size(), false);
    }
    auto sps = snapshots(services);
    // Create one copy of the shard for each service
    for (size_t i = 0; i < services.size(); i++) {
        copies.emplace_back(std::make_shared<test::TestShard>(
            copies.size(),
            services[i],
            1,  // Shard ID
            allHosts[i],
            wals[i],
            services[i]->getIOThreadPool(),
            workers,
            services[i]->getThreadManager(),
            sps[i],
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
        services[i]->addPartition(copies.back());
        copies.back()->start(getPeers(allHosts, allHosts[i], isLearner),
                             isLearner[i]);
    }

    // Wait untill all copies agree on the same leader
    waitUntilLeaderElected(copies, leader, isLearner);
}


void finishRaft(std::vector<std::shared_ptr<RaftexService>>& services,
                std::vector<std::shared_ptr<test::TestShard>>& copies,
                std::shared_ptr<thread::GenericThreadPool>& workers,
                std::shared_ptr<test::TestShard>& leader) {
    leader.reset();
    copies.clear();

    // Done test case, stop all services
    for (auto& svc : services) {
        svc->stop();
    }
    LOG(INFO) << "Stopping workers...";
    workers->stop();
    workers->wait();
    LOG(INFO) << "Waiting for all service stopped";
    for (auto& svc : services) {
        svc->waitUntilStop();
    }
}

int32_t checkLeadership(std::vector<std::shared_ptr<test::TestShard>>& copies,
                        std::shared_ptr<test::TestShard>& leader) {
    std::lock_guard<std::mutex> lock(leaderMutex);
    CHECK(!!leader);
    int32_t leaderIndex = -1;
    int i = 0;
    for (auto& c : copies) {
        if (c != nullptr && leader != c && !c->isLearner() && c->isRunning()) {
            EXPECT_EQ(leader->address(), c->leader());
        } else if (leader == c) {
            leaderIndex = i;
        }
        i++;
    }
    CHECK_GE(leaderIndex, 0);
    return leaderIndex;
}

/**
 * Check copies[index] is the leader.
 * */
void checkLeadership(std::vector<std::shared_ptr<test::TestShard>>& copies,
                     size_t index,
                     std::shared_ptr<test::TestShard>& leader) {
    std::lock_guard<std::mutex> lock(leaderMutex);
    ASSERT_FALSE(!leader);
    ASSERT_EQ(leader->address(), copies[index]->address());
}

void checkNoLeader(std::vector<std::shared_ptr<test::TestShard>>& copies) {
    for (auto& c : copies) {
        if (c != nullptr && c->isRunning()) {
            ASSERT_FALSE(c->isLeader());
        }
    }
}

void appendLogs(int start,
                int end,
                std::shared_ptr<test::TestShard> leader,
                std::vector<std::string>& msgs,
                bool waitLastLog) {
    // Append 100 logs
    LOG(INFO) << "=====> Start appending logs from index " << start << " to " << end;
    for (int i = start; i <= end; ++i) {
        msgs.emplace_back(
            folly::stringPrintf("Test Log Message %03d", i));
        auto fut = leader->appendAsync(0, msgs.back());
        if (i == end && waitLastLog) {
            fut.wait();
        }
    }
    LOG(INFO) << "<===== Finish appending logs from index " << start << " to " << end;
}

bool checkConsensus(std::vector<std::shared_ptr<test::TestShard>>& copies,
                    size_t start, size_t end,
                    std::vector<std::string>& msgs) {
    int32_t count = 0;
    for (; count < 3; count++) {
        bool concensus = true;
        // Sleep a while to make sure the last log has been committed on followers
        sleep(FLAGS_raft_heartbeat_interval_secs);

        // Check every copy
        for (auto& c : copies) {
            if (c != nullptr && c->isRunning()) {
                if (msgs.size() != c->getNumLogs() || !checkLog(c, start, end, msgs)) {
                    concensus = false;
                    break;
                }
            }
        }
        if (concensus == true) {
            return true;
        }
    }
    // Failed after retry 3 times
    EXPECT_LT(count, 3);
    return false;
}

bool checkLog(std::shared_ptr<test::TestShard>& copy,
              size_t start, size_t end,
              std::vector<std::string>& msgs) {
    for (size_t i = start; i <= end; i++) {
        folly::StringPiece msg;
        if (!copy->getLogMsg(i, msg) || msgs[i] != msg.toString()) {
            return false;
        }
    }
    return true;
}

void killOneCopy(std::vector<std::shared_ptr<RaftexService>>& services,
                 std::vector<std::shared_ptr<test::TestShard>>& copies,
                 std::shared_ptr<test::TestShard>& leader,
                 size_t index) {
    services[index]->removePartition(copies[index]);
    if (leader != nullptr && index == leader->index()) {
        std::lock_guard<std::mutex> lock(leaderMutex);
        leader.reset();
    }
    LOG(INFO) << "copies " << index << " stop";
}

void rebootOneCopy(std::vector<std::shared_ptr<RaftexService>>& services,
                   std::vector<std::shared_ptr<test::TestShard>>& copies,
                   std::vector<HostAddr> allHosts,
                   size_t index) {
    services[index]->addPartition(copies[index]);
    copies[index]->start(getPeers(allHosts, allHosts[index]));
    LOG(INFO) << "copies " << index << " reboot";
}

std::vector<std::shared_ptr<SnapshotManager>> snapshots(
                    const std::vector<std::shared_ptr<RaftexService>>& services) {
    std::vector<std::shared_ptr<SnapshotManager>> snapshots;
    for (auto& service : services) {
        std::shared_ptr<SnapshotManager> snapshot(new test::SnapshotManagerImpl(service.get()));
        snapshots.emplace_back(std::move(snapshot));
    }
    return snapshots;
}

}  // namespace raftex
}  // namespace nebula

