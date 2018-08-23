/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "raftex/RaftexService.h"
#include "raftex/test/TestShard.h"

namespace vesoft {
namespace raftex {

std::vector<HostAddr> getPeers(const std::vector<HostAddr>& all,
                               const HostAddr& self) {
    std::vector<HostAddr> peers;
    for (const auto& host : all) {
        if (host != self) {
            peers.emplace_back(host);
        }
    }

    return std::move(peers);
}


TEST(LeaderElection, ElectionAfterBoot) {
    auto svc1 = RaftexService::createService(nullptr);
    auto svc2 = RaftexService::createService(nullptr);
    auto svc3 = RaftexService::createService(nullptr);
}

}  // namespace raftex
}  // namespace vesoft


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}


