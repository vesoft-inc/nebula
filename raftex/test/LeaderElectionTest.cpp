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
#include "raftex/test/TestShard.h"

namespace vesoft {
namespace raftex {

using namespace network;
using namespace fs;

std::unique_ptr<BufferFlusher> flusher;

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

    return std::move(peers);
}


std::mutex leaderMutex;
std::condition_variable leaderCV;
std::shared_ptr<test::TestShard> leader;

void onLeaderElected(
        std::vector<std::shared_ptr<test::TestShard>>& copies,
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
        const std::vector<std::shared_ptr<test::TestShard>>& copies) {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(leaderMutex);
            leaderCV.wait(lock, [] {
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


TEST(LeaderElection, ElectionAfterBoot) {
    uint32_t ipInt;
    CHECK(NetworkUtils::ipv4ToInt("127.0.0.1", ipInt));

    std::shared_ptr<thread::GenericThreadPool> workers =
        std::make_shared<thread::GenericThreadPool>();
    workers->start(4);

    fs::TempDir walRoot("/tmp/leader_election.XXXXXX");
    std::vector<std::string> wals;
    wals.emplace_back(folly::stringPrintf("%s/copy1", walRoot.path()));
    wals.emplace_back(folly::stringPrintf("%s/copy2", walRoot.path()));
    wals.emplace_back(folly::stringPrintf("%s/copy3", walRoot.path()));
    CHECK(FileUtils::makeDir(wals[0]));
    CHECK(FileUtils::makeDir(wals[1]));
    CHECK(FileUtils::makeDir(wals[2]));


    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;

    // Service 1
    services.push_back(RaftexService::createService(nullptr));
    services.back()->waitUntilReady();
    uint16_t port = services.back()->getServerPort();
    allHosts.emplace_back(ipInt, port);

    // Service 2
    services.push_back(RaftexService::createService(nullptr));
    services.back()->waitUntilReady();
    port = services.back()->getServerPort();
    allHosts.emplace_back(ipInt, port);

    // Service 3
    services.push_back(RaftexService::createService(nullptr));
    services.back()->waitUntilReady();
    port = services.back()->getServerPort();
    allHosts.emplace_back(ipInt, port);

    std::vector<std::shared_ptr<test::TestShard>> copies;

    // Create one shard for each service
    for (size_t i = 0; i < services.size(); i++) {
        copies.emplace_back(std::make_shared<test::TestShard>(
            copies.size(),
            services[i],
            1,  // Shard ID
            allHosts[i],
            getPeers(allHosts, allHosts[i]),
            wals[i],
            flusher.get(),
            services[i]->getIOThreadPool(),
            workers,
            std::bind(&onLeadershipLost,
                      std::ref(copies),
                      std::placeholders::_1,
                      std::placeholders::_2,
                      std::placeholders::_3),
            std::bind(&onLeaderElected,
                      std::ref(copies),
                      std::placeholders::_1,
                      std::placeholders::_2,
                      std::placeholders::_3)));
        services[i]->addPartition(copies.back());
        copies.back()->start();
    }

    // Wait untill all copies agree on the same leader
    waitUntilLeaderElected(copies);

    // Check all hosts agree on the same leader
    {
        std::lock_guard<std::mutex> lock(leaderMutex);
        ASSERT_FALSE(!leader);
        for (auto& c : copies) {
            if (c != nullptr && leader != c) {
                ASSERT_EQ(leader->address(), c->leader());
            }
        }
    }

    // Let's create a new wal directory for next test
    wals.emplace_back(folly::stringPrintf("%s/copy4", walRoot.path()));
    CHECK(FileUtils::makeDir(wals[3]));

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
        getPeers(allHosts, allHosts[idx]),
        wals[3],
        flusher.get(),
        services[idx]->getIOThreadPool(),
        workers,
        std::bind(&onLeadershipLost,
                  std::ref(copies),
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3),
        std::bind(&onLeaderElected,
                  std::ref(copies),
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3)));
    services[idx]->addPartition(copies.back());
    copies.back()->start();

    // Wait untill all copies agree on the same leader
    waitUntilLeaderElected(copies);

    // Check all hosts agree on the same leader
    {
        std::lock_guard<std::mutex> lock(leaderMutex);
        ASSERT_FALSE(!leader);
        for (auto& c : copies) {
            if (c != nullptr && leader != c) {
                ASSERT_EQ(leader->address(), c->leader());
            }
        }
    }

    // Done test case, stop all services
    for (auto& svc : services) {
        svc->stop();
    }
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


