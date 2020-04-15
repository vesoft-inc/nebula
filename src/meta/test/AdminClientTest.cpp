/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include "meta/processors/admin/Balancer.h"
#include "interface/gen-cpp2/StorageService.h"
#include "fs/TempDir.h"
#include "test/ServerContext.h"
#include "meta/test/TestUtils.h"

#define RETURN_OK(req) \
    UNUSED(req); \
    do { \
        folly::Promise<storage::cpp2::AdminExecResp> pro; \
        auto f = pro.getFuture(); \
        storage::cpp2::AdminExecResp resp; \
        storage::cpp2::ResponseCommon result; \
        std::vector<storage::cpp2::ResultCode> partRetCode; \
        result.set_failed_codes(partRetCode); \
        resp.set_result(result); \
        pro.setValue(std::move(resp)); \
        return f; \
    } while (false)

#define RETURN_LEADER_CHANGED(req, leader) \
    UNUSED(req); \
    do { \
        folly::Promise<storage::cpp2::AdminExecResp> pro; \
        auto f = pro.getFuture(); \
        storage::cpp2::AdminExecResp resp; \
        storage::cpp2::ResponseCommon result; \
        std::vector<storage::cpp2::ResultCode> partRetCode; \
        storage::cpp2::ResultCode thriftRet; \
        thriftRet.set_code(storage::cpp2::ErrorCode::E_LEADER_CHANGED); \
        thriftRet.set_leader(leader); \
        partRetCode.emplace_back(std::move(thriftRet)); \
        result.set_failed_codes(partRetCode); \
        resp.set_result(result); \
        pro.setValue(std::move(resp)); \
        return f; \
    } while (false)

DECLARE_int32(max_retry_times_admin_op);

namespace nebula {
namespace meta {

class TestStorageService : public storage::cpp2::StorageServiceSvIf {
public:
    folly::Future<storage::cpp2::AdminExecResp>
    future_transLeader(const storage::cpp2::TransLeaderReq& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_addPart(const storage::cpp2::AddPartReq& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_addLearner(const storage::cpp2::AddLearnerReq& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_waitingForCatchUpData(const storage::cpp2::CatchUpDataReq& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_removePart(const storage::cpp2::RemovePartReq& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_memberChange(const storage::cpp2::MemberChangeReq& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_createCheckpoint(const storage::cpp2::CreateCPRequest& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_dropCheckpoint(const storage::cpp2::DropCPRequest& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_blockingWrites(const storage::cpp2::BlockingSignRequest& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_rebuildTagIndex(const storage::cpp2::RebuildIndexRequest& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_rebuildEdgeIndex(const storage::cpp2::RebuildIndexRequest& req) override {
        RETURN_OK(req);
    }
};

class TestStorageServiceRetry : public TestStorageService {
public:
    TestStorageServiceRetry(IPv4 ip, Port port) {
        leader_.set_ip(ip);
        leader_.set_port(port);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_addLearner(const storage::cpp2::AddLearnerReq& req) override {
        RETURN_LEADER_CHANGED(req, leader_);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_waitingForCatchUpData(const storage::cpp2::CatchUpDataReq& req) override {
        RETURN_LEADER_CHANGED(req, leader_);
    }

    folly::Future<storage::cpp2::AdminExecResp>
    future_memberChange(const storage::cpp2::MemberChangeReq& req) override {
        RETURN_LEADER_CHANGED(req, leader_);
    }

private:
    nebula::cpp2::HostAddr leader_;
};

TEST(AdminClientTest, SimpleTest) {
    auto sc = std::make_unique<test::ServerContext>();
    auto handler = std::make_shared<TestStorageService>();
    sc->mockCommon("storage", 0, handler);
    LOG(INFO) << "Start storage server on " << sc->port_;

    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    fs::TempDir rootPath("/tmp/AdminTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto client = std::make_unique<AdminClient>(kv.get());

    {
        LOG(INFO) << "Test transLeader...";
        folly::Baton<true, std::atomic> baton;
        client->transLeader(0, 0, {localIp, sc->port_}, HostAddr(1, 1))
            .thenValue([&baton](auto&&) {
            baton.post();
        });
        baton.wait();
    }
    {
        LOG(INFO) << "Test addPart...";
        folly::Baton<true, std::atomic> baton;
        client->addPart(0, 0, {localIp, sc->port_}, true).thenValue([&baton](auto&&) {
            baton.post();
        });
        baton.wait();
    }
    {
        LOG(INFO) << "Test removePart...";
        folly::Baton<true, std::atomic> baton;
        client->removePart(0, 0, {localIp, sc->port_}).thenValue([&baton](auto&&) {
            baton.post();
        });
        baton.wait();
    }
}

TEST(AdminClientTest, RetryTest) {
    auto sc1 = std::make_unique<test::ServerContext>();
    auto handler1 = std::make_shared<TestStorageService>();
    sc1->mockCommon("storage", 0, handler1);
    LOG(INFO) << "Start storage1 server on " << sc1->port_;

    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto sc2 = std::make_unique<test::ServerContext>();
    auto handler2 = std::make_shared<TestStorageServiceRetry>(localIp, sc1->port_);
    sc2->mockCommon("storage", 0, handler2);
    LOG(INFO) << "Start storage2 server on " << sc2->port_;


    LOG(INFO) << "Now test interfaces with retry to leader!";
    fs::TempDir rootPath("/tmp/AdminTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    {
        LOG(INFO) << "Write some part information!";
        std::vector<nebula::cpp2::HostAddr> thriftPeers;
        // The first peer is one broken host.
        thriftPeers.emplace_back();
        thriftPeers.back().set_ip(0);
        thriftPeers.back().set_port(0);

        // The second one is not leader.
        thriftPeers.emplace_back();
        thriftPeers.back().set_ip(localIp);
        thriftPeers.back().set_port(sc2->port_);

        // The third one is healthy.
        thriftPeers.emplace_back();
        thriftPeers.back().set_ip(localIp);
        thriftPeers.back().set_port(sc1->port_);

        std::vector<kvstore::KV> data;
        data.emplace_back(MetaServiceUtils::partKey(0, 1),
                          MetaServiceUtils::partVal(thriftPeers));
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(kDefaultSpaceId,
                          kDefaultPartId,
                          std::move(data),
                          [&baton] (kvstore::ResultCode code) {
            CHECK_EQ(kvstore::ResultCode::SUCCEEDED, code);
            baton.post();
        });
        baton.wait();
    }

    LOG(INFO) << "Now test interfaces with retry to leader!";
    auto client = std::make_unique<AdminClient>(kv.get());
    {
        LOG(INFO) << "Test transLeader, return ok if target is not leader";
        folly::Baton<true, std::atomic> baton;
        client->transLeader(0, 1, {localIp, sc2->port_}, HostAddr(1, 1))
            .thenValue([&baton](auto&& st) {
            CHECK(st.ok());
            baton.post();
        });
        baton.wait();
    }
    {
        LOG(INFO) << "Test member change...";
        folly::Baton<true, std::atomic> baton;
        client->memberChange(0, 1, HostAddr(0, 0), true).thenValue([&baton](auto&& st) {
            CHECK(st.ok());
            baton.post();
        });
        baton.wait();
    }
    {
        LOG(INFO) << "Test add learner...";
        folly::Baton<true, std::atomic> baton;
        client->addLearner(0, 1, HostAddr(0, 0)).thenValue([&baton](auto&& st) {
            CHECK(st.ok());
            baton.post();
        });
        baton.wait();
    }
    {
        LOG(INFO) << "Test waitingForCatchUpData...";
        folly::Baton<true, std::atomic> baton;
        client->waitingForCatchUpData(0, 1, HostAddr(0, 0)).thenValue([&baton](auto&& st) {
            CHECK(st.ok());
            baton.post();
        });
        baton.wait();
    }
    FLAGS_max_retry_times_admin_op = 1;
    {
        LOG(INFO) << "Test member change...";
        folly::Baton<true, std::atomic> baton;
        client->memberChange(0, 1, HostAddr(0, 0), true).thenValue([&baton](auto&& st) {
            CHECK(!st.ok());
            CHECK_EQ("Leader changed!", st.toString());
            baton.post();
        });
        baton.wait();
    }
    {
        LOG(INFO) << "Test update meta...";
        folly::Baton<true, std::atomic> baton;
        client->updateMeta(0, 1, HostAddr(0, 0), HostAddr(1, 1)).thenValue([&baton](auto&& st) {
            CHECK(st.ok());
            baton.post();
        });
        baton.wait();
        auto peersRet = client->getPeers(0, 1);
        CHECK(peersRet.ok());
        auto hosts = std::move(peersRet).value();
        ASSERT_EQ(3, hosts.size());
        ASSERT_EQ(HostAddr(localIp, sc2->port_), hosts[0]);
        ASSERT_EQ(HostAddr(localIp, sc1->port_), hosts[1]);
        ASSERT_EQ(HostAddr(1, 1), hosts[2]);
    }
}

TEST(AdminClientTest, SnapshotTest) {
    auto sc = std::make_unique<test::ServerContext>();
    auto handler = std::make_shared<TestStorageService>();
    sc->mockCommon("storage", 0, handler);
    LOG(INFO) << "Start storage1 server on " << sc->port_;

    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    LOG(INFO) << "Now test interfaces with retry to leader!";
    fs::TempDir rootPath("/tmp/admin_snapshot_test.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr(localIp, sc->port_), HostInfo(now));
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(kv.get()).size());

    std::vector<HostAddr> addresses;
    addresses.emplace_back(localIp, sc->port_);
    auto client = std::make_unique<AdminClient>(kv.get());
    {
        LOG(INFO) << "Test Blocking Writes On...";
        auto status = client->blockingWrites(1, storage::cpp2::EngineSignType::BLOCK_ON).get();
        ASSERT_TRUE(status.ok());
    }
    {
        LOG(INFO) << "Test Create Snapshot...";
        auto status = client->createSnapshot(1, "test_snapshot").get();
        ASSERT_TRUE(status.ok());
    }
    {
        LOG(INFO) << "Test Drop Snapshot...";
        auto status = client->dropSnapshot(1, "test_snapshot", addresses).get();
        ASSERT_TRUE(status.ok());
    }
    {
        LOG(INFO) << "Test Blocking Writes Off...";
        auto status = client->blockingWrites(1, storage::cpp2::EngineSignType::BLOCK_OFF).get();
        ASSERT_TRUE(status.ok());
    }
}

TEST(AdminClientTest, RebuildIndexTest) {
    auto sc = std::make_unique<test::ServerContext>();
    auto handler = std::make_shared<TestStorageService>();
    sc->mockCommon("storage", 0, handler);
    LOG(INFO) << "Start storage server on " << sc->port_;

    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    LOG(INFO) << "Now test interfaces with retry to leader!";
    fs::TempDir rootPath("/tmp/admin_snapshot_test.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr(localIp, sc->port_), HostInfo(now));
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(kv.get()).size());
    auto address = std::make_pair(localIp, sc->port_);
    auto client = std::make_unique<AdminClient>(kv.get());
    {
        LOG(INFO) << "Test Blocking Writes On...";
        auto status = client->blockingWrites(1, storage::cpp2::EngineSignType::BLOCK_ON).get();
        ASSERT_TRUE(status.ok());
    }
    {
        LOG(INFO) << "Test Rebuild Tag Index...";
        std::vector<PartitionID> parts{1, 2, 3};
        auto status = client->rebuildTagIndex(address, 1, 1, std::move(parts), false).get();
        ASSERT_TRUE(status.ok());
    }
    {
        LOG(INFO) << "Test Rebuild Edge Index...";
        std::vector<PartitionID> parts{1, 2, 3};
        auto status = client->rebuildEdgeIndex(address, 1, 1, std::move(parts), false).get();
        ASSERT_TRUE(status.ok());
    }
    {
        LOG(INFO) << "Test Blocking Writes Off...";
        auto status = client->blockingWrites(1, storage::cpp2::EngineSignType::BLOCK_OFF).get();
        ASSERT_TRUE(status.ok());
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
