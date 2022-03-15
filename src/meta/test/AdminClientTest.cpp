/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/Utils.h"
#include "interface/gen-cpp2/StorageAdminService.h"
#include "meta/test/TestUtils.h"

#define RETURN_OK(req)                                       \
  UNUSED(req);                                               \
  do {                                                       \
    folly::Promise<storage::cpp2::AdminExecResp> pro;        \
    auto f = pro.getFuture();                                \
    storage::cpp2::AdminExecResp resp;                       \
    storage::cpp2::ResponseCommon result;                    \
    std::vector<storage::cpp2::PartitionResult> partRetCode; \
    result.failed_parts_ref() = partRetCode;                 \
    resp.result_ref() = result;                              \
    pro.setValue(std::move(resp));                           \
    return f;                                                \
  } while (false)

#define RETURN_LEADER_CHANGED(req, leader)                            \
  UNUSED(req);                                                        \
  do {                                                                \
    folly::Promise<storage::cpp2::AdminExecResp> pro;                 \
    auto f = pro.getFuture();                                         \
    storage::cpp2::AdminExecResp resp;                                \
    storage::cpp2::ResponseCommon result;                             \
    std::vector<storage::cpp2::PartitionResult> partRetCode;          \
    storage::cpp2::PartitionResult thriftRet;                         \
    thriftRet.code_ref() = nebula::cpp2::ErrorCode::E_LEADER_CHANGED; \
    thriftRet.leader_ref() = leader;                                  \
    partRetCode.emplace_back(std::move(thriftRet));                   \
    result.failed_parts_ref() = partRetCode;                          \
    resp.result_ref() = result;                                       \
    pro.setValue(std::move(resp));                                    \
    return f;                                                         \
  } while (false)

DECLARE_int32(max_retry_times_admin_op);

namespace nebula {
namespace meta {

// todo(doodle): replace with gmock
class TestStorageService : public storage::cpp2::StorageAdminServiceSvIf {
 public:
  folly::Future<storage::cpp2::AdminExecResp> future_transLeader(
      const storage::cpp2::TransLeaderReq& req) override {
    RETURN_OK(req);
  }

  folly::Future<storage::cpp2::AdminExecResp> future_addPart(
      const storage::cpp2::AddPartReq& req) override {
    RETURN_OK(req);
  }

  folly::Future<storage::cpp2::AdminExecResp> future_addLearner(
      const storage::cpp2::AddLearnerReq& req) override {
    RETURN_OK(req);
  }

  folly::Future<storage::cpp2::AdminExecResp> future_waitingForCatchUpData(
      const storage::cpp2::CatchUpDataReq& req) override {
    RETURN_OK(req);
  }

  folly::Future<storage::cpp2::AdminExecResp> future_removePart(
      const storage::cpp2::RemovePartReq& req) override {
    RETURN_OK(req);
  }

  folly::Future<storage::cpp2::AdminExecResp> future_memberChange(
      const storage::cpp2::MemberChangeReq& req) override {
    RETURN_OK(req);
  }

  folly::Future<storage::cpp2::CreateCPResp> future_createCheckpoint(
      const storage::cpp2::CreateCPRequest& req) override {
    UNUSED(req);
    folly::Promise<storage::cpp2::CreateCPResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::CreateCPResp resp;
    storage::cpp2::ResponseCommon result;
    resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    nebula::cpp2::CheckpointInfo cpInfo;
    cpInfo.path_ref() = "snapshot_path";
    resp.info_ref() = {cpInfo};
    pro.setValue(std::move(resp));
    return f;
  }

  folly::Future<storage::cpp2::DropCPResp> future_dropCheckpoint(
      const storage::cpp2::DropCPRequest& req) override {
    UNUSED(req);
    folly::Promise<storage::cpp2::DropCPResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::DropCPResp resp;
    resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    pro.setValue(std::move(resp));
    return f;
  }

  folly::Future<storage::cpp2::BlockingSignResp> future_blockingWrites(
      const storage::cpp2::BlockingSignRequest& req) override {
    UNUSED(req);
    folly::Promise<storage::cpp2::BlockingSignResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::BlockingSignResp resp;
    resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    pro.setValue(std::move(resp));
    return f;
  }
};

class TestStorageServiceRetry : public TestStorageService {
 public:
  void setLeader(std::string leaderAddr, Port leaderAdminPort) {
    // when leader change returned, the port is always data port
    leader_ = Utils::getStoreAddrFromAdminAddr(HostAddr(leaderAddr, leaderAdminPort));
  }

  folly::Future<storage::cpp2::AdminExecResp> future_addLearner(
      const storage::cpp2::AddLearnerReq& req) override {
    RETURN_LEADER_CHANGED(req, leader_);
  }

  folly::Future<storage::cpp2::AdminExecResp> future_waitingForCatchUpData(
      const storage::cpp2::CatchUpDataReq& req) override {
    RETURN_LEADER_CHANGED(req, leader_);
  }

  folly::Future<storage::cpp2::AdminExecResp> future_memberChange(
      const storage::cpp2::MemberChangeReq& req) override {
    RETURN_LEADER_CHANGED(req, leader_);
  }

 private:
  HostAddr leader_;
};

TEST(AdminClientTest, SimpleTest) {
  auto rpcServer = std::make_unique<mock::RpcServer>();
  auto handler = std::make_shared<TestStorageService>();
  rpcServer->start("storage-admin", 0, handler);
  LOG(INFO) << "Start storage server on " << rpcServer->port_;

  std::string localIp("127.0.0.1");
  fs::TempDir rootPath("/tmp/AdminTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  auto client = std::make_unique<AdminClient>(kv.get());

  {
    LOG(INFO) << "Test transLeader...";
    folly::Baton<true, std::atomic> baton;
    client->transLeader(0, 0, {localIp, rpcServer->port_}, HostAddr("1", 1))
        .thenValue([&baton](auto&&) { baton.post(); });
    baton.wait();
  }
  {
    LOG(INFO) << "Test addPart...";
    folly::Baton<true, std::atomic> baton;
    client->addPart(0, 0, {localIp, rpcServer->port_}, true).thenValue([&baton](auto&&) {
      baton.post();
    });
    baton.wait();
  }
  {
    LOG(INFO) << "Test removePart...";
    folly::Baton<true, std::atomic> baton;
    client->removePart(0, 0, {localIp, rpcServer->port_}).thenValue([&baton](auto&&) {
      baton.post();
    });
    baton.wait();
  }
}

TEST(AdminClientTest, RetryTest) {
  std::string localIp("127.0.0.1");

  auto rpcServer1 = std::make_unique<mock::RpcServer>();
  auto handler1 = std::make_shared<TestStorageService>();
  rpcServer1->start("storage-admin-1", 0, handler1);
  LOG(INFO) << "Start storage server on " << rpcServer1->port_;

  auto rpcServer2 = std::make_unique<mock::RpcServer>();
  auto handler2 = std::make_shared<TestStorageServiceRetry>();
  rpcServer2->start("storage-admin-2", 0, handler2);
  handler2->setLeader(localIp, rpcServer1->port_);
  LOG(INFO) << "Start storage2 server on " << rpcServer2->port_;

  LOG(INFO) << "Now test interfaces with retry to leader!";
  fs::TempDir rootPath("/tmp/AdminTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  {
    LOG(INFO) << "Write some part information!";
    std::vector<HostAddr> thriftPeers;
    // The first peer is one broken host.
    thriftPeers.emplace_back("0", 0);

    // The second one is not leader.
    thriftPeers.emplace_back(Utils::getStoreAddrFromAdminAddr({localIp, rpcServer2->port_}));

    // The third one is healthy.
    thriftPeers.emplace_back(Utils::getStoreAddrFromAdminAddr({localIp, rpcServer1->port_}));

    std::vector<kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::partKey(0, 1), MetaKeyUtils::partVal(thriftPeers));
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&baton](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  LOG(INFO) << "Now test interfaces with retry to leader!";
  auto client = std::make_unique<AdminClient>(kv.get());
  {
    LOG(INFO) << "Test transLeader, return ok if target is not leader";
    folly::Baton<true, std::atomic> baton;
    client
        ->transLeader(
            0, 1, Utils::getStoreAddrFromAdminAddr({localIp, rpcServer2->port_}), HostAddr("1", 1))
        .thenValue([&baton](auto&& st) {
          CHECK(st.ok()) << st;
          baton.post();
        });
    baton.wait();
  }
  {
    LOG(INFO) << "Test member change...";
    folly::Baton<true, std::atomic> baton;
    client->memberChange(0, 1, HostAddr("0", 0), true).thenValue([&baton](auto&& st) {
      CHECK(st.ok()) << st;
      baton.post();
    });
    baton.wait();
  }
  {
    LOG(INFO) << "Test add learner...";
    folly::Baton<true, std::atomic> baton;
    client->addLearner(0, 1, HostAddr("0", 0)).thenValue([&baton](auto&& st) {
      CHECK(st.ok()) << st;
      baton.post();
    });
    baton.wait();
  }
  {
    LOG(INFO) << "Test waitingForCatchUpData...";
    folly::Baton<true, std::atomic> baton;
    client->waitingForCatchUpData(0, 1, HostAddr("0", 0)).thenValue([&baton](auto&& st) {
      CHECK(st.ok()) << st;
      baton.post();
    });
    baton.wait();
  }
  FLAGS_max_retry_times_admin_op = 1;
  {
    LOG(INFO) << "Test member change...";
    folly::Baton<true, std::atomic> baton;
    client->memberChange(0, 1, HostAddr("0", 0), true).thenValue([&baton](auto&& st) {
      CHECK(!st.ok());
      CHECK_EQ("Leader changed!", st.toString());
      baton.post();
    });
    baton.wait();
  }
  {
    LOG(INFO) << "Test update meta...";
    folly::Baton<true, std::atomic> baton;
    client->updateMeta(0, 1, HostAddr("0", 0), HostAddr("1", 1)).thenValue([&baton](auto&& st) {
      CHECK(st.ok()) << st;
      baton.post();
    });
    baton.wait();
    auto peersRet = client->getPeers(0, 1);
    CHECK(nebula::ok(peersRet));
    auto hosts = std::move(nebula::value(peersRet));
    ASSERT_EQ(3, hosts.size());
    ASSERT_EQ(Utils::getStoreAddrFromAdminAddr({localIp, rpcServer2->port_}), hosts[0]);
    ASSERT_EQ(Utils::getStoreAddrFromAdminAddr({localIp, rpcServer1->port_}), hosts[1]);
    ASSERT_EQ(HostAddr("1", 1), hosts[2]);
  }
}

TEST(AdminClientTest, LearnerBecomeLeaderTest) {
  // Three replica, server 1/2/3 is normal replica, server 4 is the learner at first and it
  // becomes leader
  auto rpcServer1 = std::make_unique<mock::RpcServer>();
  auto rpcServer2 = std::make_unique<mock::RpcServer>();
  auto rpcServer3 = std::make_unique<mock::RpcServer>();
  auto rpcServer4 = std::make_unique<mock::RpcServer>();
  auto handler1 = std::make_shared<TestStorageServiceRetry>();
  auto handler2 = std::make_shared<TestStorageServiceRetry>();
  auto handler3 = std::make_shared<TestStorageServiceRetry>();
  auto handler4 = std::make_shared<TestStorageService>();
  rpcServer1->start("storage-admin-1", 0, handler1);
  rpcServer2->start("storage-admin-2", 0, handler2);
  rpcServer3->start("storage-admin-3", 0, handler3);
  rpcServer4->start("storage-admin-4", 0, handler4);
  LOG(INFO) << "Start storage1 server on " << rpcServer1->port_;
  LOG(INFO) << "Start storage2 server on " << rpcServer2->port_;
  LOG(INFO) << "Start storage3 server on " << rpcServer3->port_;
  LOG(INFO) << "Start storage4 server on " << rpcServer4->port_;

  // server1 believes server2 is leader
  // server2 believes server3 is leader
  // server3 believes server4 is leader
  // so a request send to server1 need to be retried for 3 times, and be processed in server4
  std::string localIp("127.0.0.1");
  handler1->setLeader(localIp, rpcServer2->port_);
  handler2->setLeader(localIp, rpcServer3->port_);
  handler3->setLeader(localIp, rpcServer4->port_);

  LOG(INFO) << "Now test interfaces with retry to leader!";
  fs::TempDir rootPath("/tmp/AdminTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  {
    LOG(INFO) << "Write the part information, server 1/2/3 is the normal replica";
    // The request will be sent to rpcServer1 first
    std::vector<HostAddr> thriftPeers;
    thriftPeers.emplace_back(Utils::getStoreAddrFromAdminAddr({localIp, rpcServer1->port_}));
    thriftPeers.emplace_back(Utils::getStoreAddrFromAdminAddr({localIp, rpcServer2->port_}));
    thriftPeers.emplace_back(Utils::getStoreAddrFromAdminAddr({localIp, rpcServer3->port_}));

    std::vector<kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::partKey(0, 1), MetaKeyUtils::partVal(thriftPeers));
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&baton](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  LOG(INFO) << "Now test interfaces with retry to leader!";
  auto client = std::make_unique<AdminClient>(kv.get());
  for (int32_t retryTime = 0; retryTime < 3; retryTime++) {
    FLAGS_max_retry_times_admin_op = retryTime;
    LOG(INFO) << "Test member change by adding a fake host";
    folly::Baton<true, std::atomic> baton;
    client->memberChange(0, 1, HostAddr("0", 0), true).thenValue([&baton](auto&& st) {
      CHECK(!st.ok());
      CHECK_EQ("Leader changed!", st.toString());
      baton.post();
    });
    baton.wait();
  }
  FLAGS_max_retry_times_admin_op = 3;
  {
    LOG(INFO) << "Test member change by adding a fake host";
    folly::Baton<true, std::atomic> baton;
    client->memberChange(0, 1, HostAddr("0", 0), true).thenValue([&baton](auto&& st) {
      CHECK(st.ok());
      baton.post();
    });
    baton.wait();
  }
}

TEST(AdminClientTest, SnapshotTest) {
  auto rpcServer = std::make_unique<mock::RpcServer>();
  auto handler = std::make_shared<TestStorageService>();
  rpcServer->start("storage-admin", 0, handler);
  LOG(INFO) << "Start storage server on " << rpcServer->port_;

  std::string localIp("127.0.0.1");

  LOG(INFO) << "Now test interfaces with retry to leader!";
  fs::TempDir rootPath("/tmp/admin_snapshot_test.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  auto now = time::WallClock::fastNowInMilliSec();
  HostAddr host(localIp, rpcServer->port_);
  HostAddr storageHost = Utils::getStoreAddrFromAdminAddr(host);
  TestUtils::createSomeHosts(kv.get(), {storageHost});
  std::vector<kvstore::KV> times;
  ActiveHostsMan::updateHostInfo(
      kv.get(), storageHost, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""), times);
  TestUtils::doPut(kv.get(), times);
  auto hostsRet = ActiveHostsMan::getActiveHosts(kv.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(1, nebula::value(hostsRet).size());

  auto client = std::make_unique<AdminClient>(kv.get());
  {
    LOG(INFO) << "Test Blocking Writes On...";
    std::set<GraphSpaceID> ids{1};
    auto status =
        client->blockingWrites(ids, storage::cpp2::EngineSignType::BLOCK_ON, storageHost).get();
    ASSERT_TRUE(status.ok());
  }
  {
    LOG(INFO) << "Test Create Snapshot...";
    std::set<GraphSpaceID> ids{1};
    auto status = client->createSnapshot(ids, "test_snapshot", storageHost).get();
    ASSERT_TRUE(status.ok());
  }
  {
    LOG(INFO) << "Test Drop Snapshot...";
    std::set<GraphSpaceID> ids{1};
    auto status = client->dropSnapshot(ids, "test_snapshot", storageHost).get();
    ASSERT_TRUE(status.ok());
  }
  {
    LOG(INFO) << "Test Blocking Writes Off...";
    std::set<GraphSpaceID> ids{1};
    auto status =
        client->blockingWrites(ids, storage::cpp2::EngineSignType::BLOCK_OFF, storageHost).get();
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
