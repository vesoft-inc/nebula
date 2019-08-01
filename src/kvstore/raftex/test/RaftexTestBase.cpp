/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/wal/BufferFlusher.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/TestShard.h"
#include "thrift/ThriftClientManager.h"

DECLARE_uint32(heartbeat_interval);

namespace nebula {
namespace raftex {

using network::NetworkUtils;
using fs::FileUtils;
using wal::BufferFlusher;

std::unique_ptr<BufferFlusher> flusher;

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
                    << NetworkUtils::intToIPv4(host.first)
                    << ":" << host.second;
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
    while (true) {
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
            usleep(30000);

            bool sameLeader = true;
            int32_t index = 0;
            for (auto& c : copies) {
                // if a copy in abnormal state, its leader is (0, 0)
                if (!isLearner[index] && c != nullptr && leader != c && c->isRunning_ == true) {
                    if (leader->address() != c->leader()) {
                        sameLeader = false;
                        break;
                    }
                }
                index++;
            }
            if (sameLeader) {
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
            if (c != nullptr && c->isRunning_ == true) {
                if (c->leader().first == 0 && c->leader().second == 0) {
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
    IPv4 ipInt;
    CHECK(NetworkUtils::ipv4ToInt("127.0.0.1", ipInt));

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
        services.emplace_back(RaftexService::createService(nullptr));
        if (!services.back()->start())
            return;
        uint16_t port = services.back()->getServerPort();
        allHosts.emplace_back(ipInt, port);
    }

    if (isLearner.empty()) {
        isLearner.resize(allHosts.size(), false);
    }
    // Create one copy of the shard for each service
    for (size_t i = 0; i < services.size(); i++) {
        copies.emplace_back(std::make_shared<test::TestShard>(
            copies.size(),
            services[i],
            1,  // Shard ID
            allHosts[i],
            wals[i],
            flusher.get(),
            services[i]->getIOThreadPool(),
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
        services[i]->addPartition(copies.back());
        copies.back()->start(getPeers(allHosts, allHosts[i], isLearner),
                             isLearner[i]);
        copies.back()->isRunning_ = true;
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

void checkLeadership(std::vector<std::shared_ptr<test::TestShard>>& copies,
                     std::shared_ptr<test::TestShard>& leader) {
    std::lock_guard<std::mutex> lock(leaderMutex);

    ASSERT_FALSE(!leader);
    for (auto& c : copies) {
        if (c != nullptr && leader != c && !c->isLearner() && c->isRunning_ == true) {
            ASSERT_EQ(leader->address(), c->leader());
        }
    }
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
        if (c != nullptr && c->isRunning_ == true) {
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

void checkConsensus(std::vector<std::shared_ptr<test::TestShard>>& copies,
                    size_t start, size_t end,
                    std::vector<std::string>& msgs) {
    // Sleep a while to make sure the last log has been committed on followers
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies) {
        if (c != nullptr && c->isRunning_ == true) {
            ASSERT_EQ(msgs.size(), c->getNumLogs());
        }
    }
    for (size_t i = start; i <= end; i++) {
        for (auto& c : copies) {
            if (c != nullptr && c->isRunning_ == true) {
                folly::StringPiece msg;
                ASSERT_TRUE(c->getLogMsg(i, msg));
                ASSERT_EQ(msgs[i], msg.toString());
            }
        }
    }
}

void killOneCopy(std::vector<std::shared_ptr<RaftexService>>& services,
                 std::vector<std::shared_ptr<test::TestShard>>& copies,
                 std::shared_ptr<test::TestShard>& leader,
                 size_t index) {
    copies[index]->isRunning_ = false;
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
    copies[index]->isRunning_ = true;
    LOG(INFO) << "copies " << index << " reboot";
}

}  // namespace raftex
}  // namespace nebula

