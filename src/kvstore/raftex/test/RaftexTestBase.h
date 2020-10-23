/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_TEST_RAFTEXTESTBASE_H_
#define RAFTEX_TEST_RAFTEXTESTBASE_H_

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/fs/FileUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/raftex/SnapshotManager.h"
#include <gtest/gtest.h>
#include <folly/String.h>

namespace nebula {

namespace raftex {

class RaftexService;

namespace test {
class TestShard;
}  // namespace test

extern std::mutex leaderMutex;
extern std::condition_variable leaderCV;


std::vector<HostAddr> getPeers(const std::vector<HostAddr>& all,
                               const HostAddr& self,
                               std::vector<bool> isLearner = {});

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
        std::shared_ptr<test::TestShard>& leader,
        std::vector<bool> isLearner = {});

void waitUntilAllHasLeader(const std::vector<std::shared_ptr<test::TestShard>>& copies);

void setupRaft(
        int32_t numCopies,
        fs::TempDir& walRoot,
        std::shared_ptr<thread::GenericThreadPool>& workers,
        std::vector<std::string>& wals,
        std::vector<HostAddr>& allHosts,
        std::vector<std::shared_ptr<RaftexService>>& services,
        std::vector<std::shared_ptr<test::TestShard>>& copies,
        std::shared_ptr<test::TestShard>& leader,
        std::vector<bool> isLearner = {});

void finishRaft(std::vector<std::shared_ptr<RaftexService>>& services,
                std::vector<std::shared_ptr<test::TestShard>>& copies,
                std::shared_ptr<thread::GenericThreadPool>& workers,
                std::shared_ptr<test::TestShard>& leader);

int32_t checkLeadership(std::vector<std::shared_ptr<test::TestShard>>& copies,
                        std::shared_ptr<test::TestShard>& leader);

void checkLeadership(std::vector<std::shared_ptr<test::TestShard>>& copies,
                     size_t index,
                     std::shared_ptr<test::TestShard>& leader);
void checkNoLeader(std::vector<std::shared_ptr<test::TestShard>>& copies);

void appendLogs(int start,
                int end,
                std::shared_ptr<test::TestShard> leader,
                std::vector<std::string>& msgs,
                bool waitLastLog = false);

bool checkConsensus(std::vector<std::shared_ptr<test::TestShard>>& copies,
                    size_t start, size_t end,
                    std::vector<std::string>& msgs);

bool checkLog(std::shared_ptr<test::TestShard>& copy,
              size_t start, size_t end,
              std::vector<std::string>& msgs);

void killOneCopy(std::vector<std::shared_ptr<RaftexService>>& services,
                 std::vector<std::shared_ptr<test::TestShard>>& copies,
                 std::shared_ptr<test::TestShard>& leader,
                 size_t index);

void rebootOneCopy(std::vector<std::shared_ptr<RaftexService>>& services,
                   std::vector<std::shared_ptr<test::TestShard>>& copies,
                   std::vector<HostAddr> allHosts,
                   size_t index);

std::vector<std::shared_ptr<SnapshotManager>> snapshots(
                   const std::vector<std::shared_ptr<RaftexService>>& services);

class RaftexTestFixture : public ::testing::Test {
public:
    explicit RaftexTestFixture(const std::string& testName, int32_t size = 3)
        : testName_(testName)
        , size_(size) {}
    ~RaftexTestFixture() = default;

    void SetUp() override {
        walRoot_ = std::make_unique<fs::TempDir>(
            folly::stringPrintf("/tmp/%s.XXXXXX", testName_.c_str()).c_str());
        setupRaft(size_, *walRoot_, workers_, wals_, allHosts_, services_, copies_, leader_);

        // Check all hosts agree on the same leader
        checkLeadership(copies_, leader_);
    }

    void TearDown() override {
        finishRaft(services_, copies_, workers_, leader_);
        walRoot_.reset();
    }

protected:
    const std::string testName_;
    int32_t size_;
    std::unique_ptr<fs::TempDir> walRoot_;
    std::shared_ptr<thread::GenericThreadPool> workers_;
    std::vector<std::string> wals_;
    std::vector<HostAddr> allHosts_;
    std::vector<std::shared_ptr<RaftexService>> services_;
    std::vector<std::shared_ptr<test::TestShard>> copies_;
    std::shared_ptr<test::TestShard> leader_;
    std::vector<std::shared_ptr<SnapshotManager>> snapshots_;
};

}  // namespace raftex
}  // namespace nebula
#endif  // RAFTEX_TEST_RAFTEXTESTBASE_H_

