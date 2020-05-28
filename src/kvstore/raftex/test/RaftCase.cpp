/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/fs/FileUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/test/RaftexTestBase.h"
#include "kvstore/raftex/test/TestShard.h"
#include <gtest/gtest.h>
#include <folly/String.h>

DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace raftex {

class ThreeRaftTest : public RaftexTestFixture {
public:
    ThreeRaftTest() : RaftexTestFixture("three_raft_test") {}
};

class FiveRaftTest : public RaftexTestFixture {
public:
    FiveRaftTest() : RaftexTestFixture("five_raft_test", 5) {}
};

TEST_F(ThreeRaftTest, LeaderCrashReboot) {
    FLAGS_raft_heartbeat_interval_secs = 1;
    LOG(INFO) << "=====> Start leaderCrash test";

    LOG(INFO) << "=====> Now let's kill the old leader";
    size_t idx = leader_->index();
    killOneCopy(services_, copies_, leader_, idx);

    // Wait untill all copies agree on the same leader_
    waitUntilLeaderElected(copies_, leader_);
    // Check all hosts agree on the same leader_
    checkLeadership(copies_, leader_);

    LOG(INFO) << "=====> Now old leader comes back";
    // Crashed leader reboot
    rebootOneCopy(services_, copies_, allHosts_, idx);

    waitUntilAllHasLeader(copies_);
    checkLeadership(copies_, leader_);

    LOG(INFO) << "=====> Now let's kill leader and a follower, no quorum";
    idx = leader_->index();
    killOneCopy(services_, copies_, leader_, idx);
    killOneCopy(services_, copies_, leader_, (idx + 1) % copies_.size());
    sleep(FLAGS_raft_heartbeat_interval_secs);
    checkNoLeader(copies_);

    LOG(INFO) << "=====> Now one of dead copy rejoin, quorum arises";
    rebootOneCopy(services_, copies_, allHosts_, idx);
    waitUntilLeaderElected(copies_, leader_);

    LOG(INFO) << "=====> Now all copy rejoin, should not disrupt leader";
    rebootOneCopy(services_, copies_, allHosts_, idx);
    sleep(FLAGS_raft_heartbeat_interval_secs);
    waitUntilAllHasLeader(copies_);
    checkLeadership(copies_, leader_);

    LOG(INFO) << "<===== Done leaderCrash test";
}

TEST_F(ThreeRaftTest, ConsensusWhenFollowDisconnect) {
    LOG(INFO) << "=====> Start ConsensusWhenFollowDisconnect test";

    std::vector<std::string> msgs;
    appendLogs(0, 0, leader_, msgs);
    checkConsensus(copies_, 0, 0, msgs);

    // Let's kill one follower
    LOG(INFO) << "=====> Now let's kill one follower";
    size_t idx = (leader_->index() + 1) % (copies_.size());
    killOneCopy(services_, copies_, leader_, idx);

    // Check all hosts agree on the same leader
    checkLeadership(copies_, leader_);
    appendLogs(1, 4, leader_, msgs);
    checkConsensus(copies_, 1, 4, msgs);

    LOG(INFO) << "=====> Now follower comes back";
    // Crashed follower reboot
    rebootOneCopy(services_, copies_, allHosts_, idx);

    waitUntilAllHasLeader(copies_);
    checkLeadership(copies_, leader_);

    appendLogs(5, 6, leader_, msgs);
    checkConsensus(copies_, 5, 6, msgs);

    LOG(INFO) << "<===== Done ConsensusWhenFollowDisconnect test";
}

TEST_F(ThreeRaftTest, LeaderCrashRebootWithLogs) {
    LOG(INFO) << "=====> Start LeaderNetworkFailure test";

    std::vector<std::string> msgs;
    appendLogs(0, 0, leader_, msgs);
    checkConsensus(copies_, 0, 0, msgs);

    LOG(INFO) << "=====> Now leader of term 1 disconnect";
    auto leader1 = leader_;
    killOneCopy(services_, copies_, leader_, leader_->index());

    LOG(INFO) << "=====> Wait until leader of term 2 elected";
    // Wait untill all copies agree on the same leader_
    waitUntilLeaderElected(copies_, leader_);
    auto leader2 = leader_;
    ASSERT_NE(leader1, leader2);

    appendLogs(1, 3, leader_, msgs);
    checkConsensus(copies_, 1, 3, msgs);

    LOG(INFO) << "=====> Now leader of term 2 disconnect, no quorum";
    killOneCopy(services_, copies_, leader_, leader_->index());

    LOG(INFO) << "=====> Now leader of term 1 reconnect, quorum arises";
    rebootOneCopy(services_, copies_, allHosts_, leader1->index());
    waitUntilLeaderElected(copies_, leader_);
    // only leader3 can be elected as leader
    auto leader3 = leader_;
    ASSERT_NE(leader1, leader3);
    ASSERT_NE(leader2, leader3);

    appendLogs(4, 6, leader_, msgs);
    checkConsensus(copies_, 4, 6, msgs);

    LOG(INFO) << "=====> Now leader of term 2 reconnect, should not disrupt leader";
    rebootOneCopy(services_, copies_, allHosts_, leader2->index());
    appendLogs(7, 9, leader_, msgs);
    checkConsensus(copies_, 7, 9, msgs);
    checkLeadership(copies_, leader3);

    LOG(INFO) << "<===== Done LeaderNetworkFailure test";
}

TEST_F(ThreeRaftTest, Persistance) {
    LOG(INFO) << "=====> Start persistance test";

    LOG(INFO) << "=====> Now let's kill random copy";
    std::vector<std::string> msgs;
    for (int32_t i = 0; i < 10; i++) {
        appendLogs(i, i, leader_, msgs, true);
        // randomly kill one copy and restart
        size_t idx = folly::Random::rand32(3);
        bool waitForNewLeader = false;
        if (idx == leader_->index()) {
            waitForNewLeader = true;
        }
        killOneCopy(services_, copies_, leader_, idx);
        rebootOneCopy(services_, copies_, allHosts_, idx);
        if (waitForNewLeader) {
            waitUntilAllHasLeader(copies_);
        }
    }
    sleep(FLAGS_raft_heartbeat_interval_secs);
    checkConsensus(copies_, 0, 9, msgs);
    LOG(INFO) << "<===== Done persistance test";
}

// this ut will lead to crash in some case, see issue #685
TEST_F(FiveRaftTest, DISABLED_Figure8) {
    // Test the scenarios described in Figure 8 of the extended Raft paper.
    // The purpose is to test leader in a new term may try to finish replicating log entries
    // of previous term that haven't been committed yet. So we append log by leader (may or may
    // not succeed), and then crash and reboot, so the term will increase because of leadership
    // change, new leader should commit logs of previous term when its appendLogs of current term
    // has been accepted by quorum.
    LOG(INFO) << "=====> Start figure 8 test";
    int32_t alive = size_;
    int32_t quorum = (size_ + 1) / 2;

    for (int32_t i = 0; i < 500; i++) {
        decltype(leader_) leader;
        {
            std::lock_guard<std::mutex> lock(leaderMutex);
            if (leader_ != nullptr) {
                leader = leader_;
            }
        }
        if (leader != nullptr) {
            if (folly::Random::rand32(1000) < 100) {
                leader->appendAsync(0, folly::stringPrintf("Log Message probably ok %03d", i));
                usleep(300000);
            } else {
                leader->appendAsync(0, folly::stringPrintf("Log Message probably failed %03d", i));
                usleep(10000);
            }
        }

        if (leader != nullptr) {
            killOneCopy(services_, copies_, leader, leader->index());
            --alive;
        }

        if (alive < quorum) {
            size_t idx = folly::Random::rand32(size_);
            if (!copies_[idx]->isRunning()) {
                rebootOneCopy(services_, copies_, allHosts_, idx);
                ++alive;
            }
        }
    }

    LOG(INFO) << "=====> Now let's reboot all copy and check consensus";
    for (int32_t i = 0; i < size_; i++) {
        if (!copies_[i]->isRunning()) {
            rebootOneCopy(services_, copies_, allHosts_, i);
        }
    }

    // wait until all has been committed
    sleep(5 * FLAGS_raft_heartbeat_interval_secs);
    size_t count = leader_->getNumLogs();
    LOG(INFO) << "=====> Check all copies have " << count << " logs";

    // Check every copy
    for (auto& c : copies_) {
        ASSERT_EQ(count, c->getNumLogs());
    }
    for (size_t i = 0; i < count; i++) {
        folly::StringPiece expected;
        leader_->getLogMsg(i, expected);
        for (auto& c : copies_) {
            folly::StringPiece msg;
            ASSERT_TRUE(c->getLogMsg(i, msg));
            ASSERT_EQ(expected, msg);
        }
    }

    LOG(INFO) << "<===== Done leaderCrash test";
}

}  // namespace raftex
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
