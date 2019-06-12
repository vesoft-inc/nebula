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

namespace nebula {
namespace raftex {

using network::NetworkUtils;
using fs::FileUtils;
using wal::BufferFlusher;

std::unique_ptr<BufferFlusher> flusher;

std::mutex leaderMutex;
std::condition_variable leaderCV;


std::vector<HostAddr> getPeers(const std::vector<HostAddr>& all,
                               const HostAddr& self) {
    std::vector<HostAddr> peers;
    for (const auto& host : all) {
        if (host != self) {
            VLOG(2) << "Adding host "
                    << NetworkUtils::intToIPv4(host.first)
                    << ":" << host.second;
            peers.emplace_back(host);
        }
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
        std::shared_ptr<test::TestShard>& leader) {
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

            bool sameLeader = true;
            for (auto& c : copies) {
                if (c != nullptr && leader != c) {
                    if (leader->address() != c->leader()) {
                        sameLeader = false;
                        break;
                    }
                }
            }
            if (sameLeader) {
                break;
            }
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
        std::shared_ptr<test::TestShard>& leader) {
    uint32_t ipInt;
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
        services.push_back(RaftexService::createService(nullptr));
        services.back()->waitUntilReady();
        uint16_t port = services.back()->getServerPort();
        allHosts.emplace_back(ipInt, port);
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
        copies.back()->start(getPeers(allHosts, allHosts[i]));
    }

    // Wait untill all copies agree on the same leader
    waitUntilLeaderElected(copies, leader);
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
    workers->stop();
    workers->wait();
    for (auto& svc : services) {
        svc->waitUntilStop();
    }
}


void checkLeadership(std::vector<std::shared_ptr<test::TestShard>>& copies,
                     std::shared_ptr<test::TestShard>& leader) {
    std::lock_guard<std::mutex> lock(leaderMutex);

    ASSERT_FALSE(!leader);
    for (auto& c : copies) {
        if (c != nullptr && leader != c) {
            ASSERT_EQ(leader->address(), c->leader());
        }
    }
}

}  // namespace raftex
}  // namespace nebula

