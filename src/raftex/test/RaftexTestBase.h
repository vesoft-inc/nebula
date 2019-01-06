/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_TEST_RAFTEXTESTBASE_H_
#define RAFTEX_TEST_RAFTEXTESTBASE_H_

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include "fs/TempDir.h"
#include "fs/FileUtils.h"
#include "thread/GenericThreadPool.h"
#include "network/NetworkUtils.h"


namespace nebula {

namespace wal {
class BufferFlusher;
}  // namespace wal


namespace raftex {

class RaftexService;

namespace test {
class TestShard;
}  // namespace test

extern std::unique_ptr<wal::BufferFlusher> flusher;

extern std::mutex leaderMutex;
extern std::condition_variable leaderCV;


std::vector<HostAddr> getPeers(const std::vector<HostAddr>& all,
                               const HostAddr& self);

void onLeaderElected(
        std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader,
        size_t idx,
        const char* idStr,
        TermID term);

void onLeadershipLost(
        std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader,
        size_t idx,
        const char* idStr,
        TermID term);

void waitUntilLeaderElected(
        const std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader);

void setupRaft(
        fs::TempDir& walRoot,
        std::shared_ptr<thread::GenericThreadPool>& workers,
        std::vector<std::string>& wals,
        std::vector<HostAddr>& allHosts,
        std::vector<std::shared_ptr<RaftexService>>& services,
        std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader);

void finishRaft(std::vector<std::shared_ptr<RaftexService>>& services,
                std::vector<std::shared_ptr<test::TestShard>>& copies,
                std::shared_ptr<thread::GenericThreadPool>& workers,
                std::shared_ptr<test::TestShard>& leader);

void checkLeadership(std::vector<std::shared_ptr<test::TestShard>>& copies,
                     std::shared_ptr<test::TestShard>& leader);

}  // namespace raftex
}  // namespace nebula
#endif  // RAFTEX_TEST_RAFTEXTESTBASE_H_

