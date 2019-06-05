/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
        int32_t numCopies,
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


class RaftexTestFixture : public ::testing::Test {
public:
    explicit RaftexTestFixture(const std::string& testName)
        : testName_(testName) {}
    ~RaftexTestFixture() = default;

    void SetUp() override {
        walRoot_ = std::make_unique<fs::TempDir>(
            folly::stringPrintf("/tmp/%s.XXXXXX", testName_.c_str()).c_str());
        setupRaft(3, *walRoot_, workers_, wals_, allHosts_, services_, copies_, leader_);

        // Check all hosts agree on the same leader
        checkLeadership(copies_, leader_);
    }

    void TearDown() override {
        finishRaft(services_, copies_, workers_, leader_);
        walRoot_.reset();
    }

protected:
    const std::string testName_;
    std::unique_ptr<fs::TempDir> walRoot_;
    std::shared_ptr<thread::GenericThreadPool> workers_;
    std::vector<std::string> wals_;
    std::vector<HostAddr> allHosts_;
    std::vector<std::shared_ptr<RaftexService>> services_;
    std::vector<std::shared_ptr<test::TestShard>> copies_;

    std::shared_ptr<test::TestShard> leader_;
};

}  // namespace raftex
}  // namespace nebula
#endif  // RAFTEX_TEST_RAFTEXTESTBASE_H_

