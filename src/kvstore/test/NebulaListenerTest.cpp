/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/fs/TempDir.h"
#include "common/meta/Common.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/wal/AtomicLogBuffer.h"
#include "meta/ActiveHostsMan.h"

DECLARE_uint32(raft_heartbeat_interval_secs);
DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_int32(listener_pursue_leader_threshold);
DECLARE_int32(clean_wal_interval_secs);
DECLARE_uint32(snapshot_part_rate_limit);
DECLARE_uint32(snapshot_batch_size);

using nebula::meta::ListenerHosts;
using nebula::meta::PartHosts;

namespace nebula {
namespace kvstore {

class DummyListener : public Listener {
 public:
  DummyListener(GraphSpaceID spaceId,
                PartitionID partId,
                HostAddr localAddr,
                const std::string& walPath,
                std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                std::shared_ptr<thread::GenericThreadPool> workers,
                std::shared_ptr<folly::Executor> handlers,
                meta::SchemaManager* schemaMan)
      : Listener(spaceId,
                 partId,
                 localAddr,
                 walPath,
                 ioPool,
                 workers,
                 handlers,
                 nullptr,
                 nullptr,
                 nullptr,
                 schemaMan) {}

  std::vector<KV> data() {
    return data_;
  }

  std::tuple<cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                                               LogID committedLogId,
                                                               TermID committedLogTerm,
                                                               bool finished) override {
    bool unl = raftLock_.try_lock();
    auto result = Listener::commitSnapshot(data, committedLogId, committedLogTerm, finished);
    if (unl) {
      raftLock_.unlock();
    }
    committedSnapshot_.first += std::get<1>(result);
    committedSnapshot_.second += std::get<2>(result);
    snapshotBatchCount_++;
    return result;
  }

  std::pair<int64_t, int64_t> committedSnapshot() {
    return committedSnapshot_;
  }

  std::pair<LogID, TermID> committedId() {
    return lastCommittedLogId();
  }

  int32_t snapshotBatchCount() {
    return snapshotBatchCount_;
  }

 protected:
  void init() override {}

  bool apply(const std::vector<KV>& kvs) override {
    for (const auto& kv : kvs) {
      data_.emplace_back(kv);
    }
    return true;
  }

  bool persist(LogID, TermID, LogID) override {
    return true;
  }

  std::pair<LogID, TermID> lastCommittedLogId() override {
    return std::make_pair(committedLogId_, lastLogTerm_);
  }

  LogID lastApplyLogId() override {
    return lastApplyLogId_;
  }

  nebula::cpp2::ErrorCode cleanup() override {
    data_.clear();
    leaderCommitId_ = 0;
    lastApplyLogId_ = 0;
    snapshotBatchCount_ = 0;
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 private:
  std::vector<KV> data_;
  std::pair<int64_t, int64_t> committedSnapshot_{0, 0};
  int32_t snapshotBatchCount_{0};
};

class ListenerBasicTest : public ::testing::TestWithParam<std::tuple<int32_t, int32_t, int32_t>> {
 public:
  void SetUp() override {
    auto param = GetParam();
    partCount_ = std::get<0>(param);
    replicas_ = std::get<1>(param);
    listenerCount_ = std::get<2>(param);

    rootPath_ = std::make_unique<fs::TempDir>("/tmp/nebula_store_test.XXXXXX");
    getAvailablePort();
    initDataReplica();
    initListenerReplica();
    startDummyListener();

    LOG(INFO) << "Waiting for all leaders elected!";
    waitLeader();
  }

  void TearDown() override {
    for (const auto& store : stores_) {
      store->stop();
    }
    for (const auto& listener : listeners_) {
      listener->stop();
    }
  }

 protected:
  void getAvailablePort() {
    std::string ip("127.0.0.1");
    for (int32_t i = 0; i < replicas_; i++) {
      peers_.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }
    for (int32_t i = 0; i < listenerCount_; i++) {
      listenerHosts_.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }
  }

  void initDataReplica() {
    LOG(INFO) << "Init data replica";
    for (int32_t i = 0; i < replicas_; i++) {
      stores_.emplace_back(initStore(i, listenerHosts_));
      stores_.back()->init();
    }
  }

  void initListenerReplica() {
    LOG(INFO) << "Init listener replica by manual";
    for (int32_t i = 0; i < listenerCount_; i++) {
      listeners_.emplace_back(initListener(i));
      listeners_.back()->init();
      listeners_.back()->spaceListeners_.emplace(spaceId_, std::make_shared<SpaceListenerInfo>());
    }
  }

  std::unique_ptr<NebulaStore> initStore(int32_t index,
                                         const std::vector<HostAddr>& listeners = {}) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    for (int32_t partId = 1; partId <= partCount_; partId++) {
      PartHosts ph;
      ph.spaceId_ = spaceId_;
      ph.partId_ = partId;
      ph.hosts_ = peers_;
      partMan->partsMap_[spaceId_][partId] = std::move(ph);
      // add remote listeners
      if (!listeners.empty()) {
        partMan->remoteListeners_[spaceId_][partId].emplace_back(
            listeners[partId % listeners.size()], meta::cpp2::ListenerType::UNKNOWN);
      }
    }

    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk%d", rootPath_->path(), index));

    KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    HostAddr local = peers_[index];
    return std::make_unique<NebulaStore>(std::move(options), ioThreadPool, local, getWorkers());
  }

  std::unique_ptr<NebulaStore> initListener(int32_t index) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    KVOptions options;
    options.listenerPath_ = folly::stringPrintf("%s/listener%d", rootPath_->path(), index);
    options.partMan_ = std::move(partMan);
    HostAddr local = listenerHosts_[index];
    return std::make_unique<NebulaStore>(std::move(options), ioThreadPool, local, getWorkers());
  }

  void startDummyListener() {
    // start dummy listener on listener hosts
    for (int32_t partId = 1; partId <= partCount_; partId++) {
      // count which listener holds this part
      auto index = partId % listenerHosts_.size();
      auto walPath = folly::stringPrintf(
          "%s/listener%lu/%d/%d/wal", rootPath_->path(), index, spaceId_, partId);
      auto local = NebulaStore::getRaftAddr(listenerHosts_[index]);
      auto dummy = std::make_shared<DummyListener>(spaceId_,
                                                   partId,
                                                   local,
                                                   walPath,
                                                   listeners_[index]->ioPool_,
                                                   listeners_[index]->bgWorkers_,
                                                   listeners_[index]->workers_,
                                                   nullptr);
      listeners_[index]->raftService_->addPartition(dummy);
      std::vector<HostAddr> raftPeers;
      std::transform(
          peers_.begin(), peers_.end(), std::back_inserter(raftPeers), [](const auto& host) {
            return NebulaStore::getRaftAddr(host);
          });
      dummy->start(std::move(raftPeers));
      listeners_[index]->spaceListeners_[spaceId_]->listeners_[partId].emplace(
          meta::cpp2::ListenerType::UNKNOWN, dummy);
      dummies_.emplace(partId, dummy);
    }
  }

  std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> getWorkers() {
    auto worker = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1);
    worker->setNamePrefix("executor");
    worker->start();
    return worker;
  }

  void waitLeader() {
    while (true) {
      int32_t leaderCount = 0;
      for (int i = 0; i < replicas_; i++) {
        nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
        leaderCount += stores_[i]->allLeader(leaderIds);
      }
      if (leaderCount == partCount_) {
        break;
      }
      usleep(100000);
    }
  }

  HostAddr findLeader(PartitionID partId) {
    while (true) {
      auto leaderRet = stores_[0]->partLeader(spaceId_, partId);
      CHECK(ok(leaderRet));
      auto leader = value(std::move(leaderRet));
      if (leader == HostAddr("", 0)) {
        sleep(1);
        continue;
      }
      return leader;
    }
  }

  size_t findStoreIndex(const HostAddr& addr) {
    for (size_t i = 0; i < peers_.size(); i++) {
      if (peers_[i] == addr) {
        return i;
      }
    }
    LOG(FATAL) << "Should not reach here!";
    return 0;
  }

 protected:
  int32_t partCount_;
  int32_t replicas_;
  int32_t listenerCount_;

  std::unique_ptr<fs::TempDir> rootPath_;
  GraphSpaceID spaceId_ = 1;
  std::vector<HostAddr> peers_;
  std::vector<HostAddr> listenerHosts_;
  std::vector<std::unique_ptr<NebulaStore>> stores_;
  std::vector<std::unique_ptr<NebulaStore>> listeners_;
  // dummies_ is a copy of Listener in listeners_, for convenience to check
  // consensus
  std::unordered_map<PartitionID, std::shared_ptr<DummyListener>> dummies_;
};

class ListenerAdvanceTest : public ListenerBasicTest {
 public:
  void SetUp() override {
    FLAGS_wal_ttl = 3;
    FLAGS_wal_file_size = 128;
    FLAGS_wal_buffer_size = 1024;
    FLAGS_listener_pursue_leader_threshold = 0;
    FLAGS_clean_wal_interval_secs = 3;
    FLAGS_raft_heartbeat_interval_secs = 5;
    ListenerBasicTest::SetUp();
  }
};

class ListenerSnapshotTest : public ListenerAdvanceTest {
 public:
  void SetUp() override {
    FLAGS_snapshot_part_rate_limit = 40 * 1024;
    FLAGS_snapshot_batch_size = 10 * 1024;
    ListenerAdvanceTest::SetUp();
  }
};

TEST_P(ListenerBasicTest, SimpleTest) {
  LOG(INFO) << "Insert some data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 100; i++) {
      data.emplace_back(folly::stringPrintf("key_%d_%d", partId, i),
                        folly::stringPrintf("val_%d_%d", partId, i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  // wait listener commit
  sleep(FLAGS_raft_heartbeat_interval_secs);

  LOG(INFO) << "Check listener's data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto dummy = dummies_[partId];
    const auto& data = dummy->data();
    CHECK_EQ(100, data.size());
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
      CHECK_EQ(folly::stringPrintf("key_%d_%d", partId, i), data[i].first);
      CHECK_EQ(folly::stringPrintf("val_%d_%d", partId, i), data[i].second);
    }
  }
}

TEST_P(ListenerBasicTest, TransLeaderTest) {
  LOG(INFO) << "Insert some data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 100; i++) {
      data.emplace_back(folly::stringPrintf("key_%d_%d", partId, i),
                        folly::stringPrintf("val_%d_%d", partId, i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  LOG(INFO) << "Transfer all part leader to first replica";
  auto targetAddr = NebulaStore::getRaftAddr(peers_[0]);
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    folly::Baton<true, std::atomic> baton;
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    auto partRet = stores_[index]->part(spaceId_, partId);
    CHECK(ok(partRet));
    auto part = value(partRet);
    part->asyncTransferLeader(targetAddr, [&](cpp2::ErrorCode) { baton.post(); });
    baton.wait();
  }
  sleep(FLAGS_raft_heartbeat_interval_secs);
  {
    nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
    ASSERT_EQ(partCount_, stores_[0]->allLeader(leaderIds));
  }

  LOG(INFO) << "Insert more data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 100; i < 200; i++) {
      data.emplace_back(folly::stringPrintf("key_%d_%d", partId, i),
                        folly::stringPrintf("val_%d_%d", partId, i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  // wait listener commit
  sleep(FLAGS_raft_heartbeat_interval_secs);

  LOG(INFO) << "Check listener's data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto dummy = dummies_[partId];
    const auto& data = dummy->data();
    CHECK_EQ(200, data.size());
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
      CHECK_EQ(folly::stringPrintf("key_%d_%d", partId, i), data[i].first);
      CHECK_EQ(folly::stringPrintf("val_%d_%d", partId, i), data[i].second);
    }
  }
}

TEST_P(ListenerBasicTest, CommitSnapshotTest) {
  LOG(INFO) << "Add some data to commit snapshot.";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 100; i++) {
      data.emplace_back(folly::stringPrintf("key_%d_%d", partId, i),
                        folly::stringPrintf("val_%d_%d", partId, i));
    }
    std::vector<std::string> rows;
    int64_t size = 0;
    for (auto kv : data) {
      auto kvStr = encodeKV(kv.first, kv.second);
      size += kvStr.size();
      rows.emplace_back(kvStr);
    }
    auto dummy = dummies_[partId];
    auto ret = dummy->commitSnapshot(rows, 100, 1, true);
    EXPECT_EQ(std::get<0>(ret), nebula::cpp2::ErrorCode::SUCCEEDED);
    EXPECT_EQ(std::get<1>(ret), 100);
    EXPECT_EQ(std::get<2>(ret), size);
  }

  LOG(INFO) << "Check listener's data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto dummy = dummies_[partId];
    const auto& data = dummy->data();
    CHECK_EQ(100, data.size());
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
      CHECK_EQ(folly::stringPrintf("key_%d_%d", partId, i), data[i].first);
      CHECK_EQ(folly::stringPrintf("val_%d_%d", partId, i), data[i].second);
    }
  }
}

TEST_P(ListenerBasicTest, ListenerResetByWalTest) {
  FLAGS_wal_ttl = 14400;
  FLAGS_wal_file_size = 1024;
  FLAGS_wal_buffer_size = 512;
  FLAGS_listener_pursue_leader_threshold = 0;
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 100000; i++) {
      data.emplace_back(folly::stringPrintf("key_%d_%d", partId, i),
                        folly::stringPrintf("val_%d_%d", partId, i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  // wait listener commit
  sleep(FLAGS_raft_heartbeat_interval_secs + 3);

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto dummy = dummies_[partId];
    const auto& data = dummy->data();
    CHECK_EQ(100000, data.size());
  }

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    dummies_[partId]->resetListener();
    CHECK_EQ(0, dummies_[partId]->data().size());
    CHECK_EQ(0, dummies_[partId]->getApplyId());
  }

  sleep(FLAGS_raft_heartbeat_interval_secs + 3);

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    while (true) {
      if (dummies_[partId]->pursueLeaderDone()) {
        break;
      }
    }
  }

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto dummy = dummies_[partId];
    const auto& data = dummy->data();
    CHECK_EQ(100000, data.size());
  }
}

TEST_P(ListenerAdvanceTest, ListenerResetBySnapshotTest) {
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    for (int32_t i = 0; i < 10; i++) {
      std::vector<KV> data;
      for (int32_t j = 0; j < 1000; j++) {
        auto vKey = NebulaKeyUtils::tagKey(8, partId, folly::to<std::string>(i * 1000 + j), 5);
        data.emplace_back(std::move(vKey), folly::stringPrintf("val_%d_%d", partId, i * 1000 + j));
      }
      auto leader = findLeader(partId);
      auto index = findStoreIndex(leader);
      folly::Baton<true, std::atomic> baton;
      stores_[index]->asyncMultiPut(
          spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
          });
      baton.wait();
    }
  }

  // wait listener commit
  sleep(2 * FLAGS_raft_heartbeat_interval_secs);

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto dummy = dummies_[partId];
    const auto& data = dummy->data();
    CHECK_EQ(10000, data.size());
  }

  // sleep enough time, wait ttl is expired
  sleep(FLAGS_clean_wal_interval_secs + FLAGS_wal_ttl + 1);

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    auto res = stores_[index]->part(spaceId_, partId);
    CHECK(ok(res));
    auto part = value(std::move(res));
    // leader has trigger cleanWAL at this point, so firstLogId in wal will > 1
    CHECK_GT(part->wal()->firstLogId(), 1);
    // clean the wal buffer to make sure snapshot will be pulled
    part->wal()->buffer()->reset();
  }

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    dummies_[partId]->resetListener();
    CHECK_EQ(0, dummies_[partId]->getApplyId());
    auto termAndId = dummies_[partId]->committedId();
    CHECK_EQ(0, termAndId.first);
    CHECK_EQ(0, termAndId.second);
  }

  sleep(FLAGS_raft_heartbeat_interval_secs + 1);

  std::vector<bool> partResult;
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto retry = 0;
    while (retry++ < 6) {
      auto result = dummies_[partId]->committedSnapshot();
      if (result.first >= 10000) {
        partResult.emplace_back(true);
        ASSERT_EQ(10000, dummies_[partId]->data().size());
        break;
      }
      usleep(1000);
    }
    CHECK_LT(retry, 6);
  }
  CHECK_EQ(partResult.size(), partCount_);
}

TEST_P(ListenerSnapshotTest, SnapshotRateLimitTest) {
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    // Write 10000 kvs in a part, key size is sizeof(partId) + vId + tagId = 4 + 8 + 4 = 16,
    // value size is 24, so total size of a kv is 40. The snapshot size of a partition will be
    // around 400Kb, and the rate limit is set to 40Kb, so snapshot will be sent at least 10
    // seconds.
    for (int32_t i = 0; i < 10; i++) {
      std::vector<KV> data;
      for (int32_t j = 0; j < 1000; j++) {
        auto vKey = NebulaKeyUtils::tagKey(8, partId, folly::to<std::string>(i * 1000 + j), 5);
        data.emplace_back(std::move(vKey), std::string(24, 'X'));
      }
      auto leader = findLeader(partId);
      auto index = findStoreIndex(leader);
      folly::Baton<true, std::atomic> baton;
      stores_[index]->asyncMultiPut(
          spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
          });
      baton.wait();
    }
  }

  // wait listener commit
  sleep(2 * FLAGS_raft_heartbeat_interval_secs);

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto dummy = dummies_[partId];
    const auto& data = dummy->data();
    CHECK_EQ(10000, data.size());
  }

  // sleep enough time, wait ttl is expired
  sleep(FLAGS_clean_wal_interval_secs + FLAGS_wal_ttl + 1);

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    auto res = stores_[index]->part(spaceId_, partId);
    CHECK(ok(res));
    auto part = value(std::move(res));
    // leader has trigger cleanWAL at this point, so firstLogId in wal will > 1
    CHECK_GT(part->wal()->firstLogId(), 1);
    // clean the wal buffer to make sure snapshot will be pulled
    part->wal()->buffer()->reset();
  }

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    dummies_[partId]->resetListener();
    CHECK_EQ(0, dummies_[partId]->getApplyId());
    auto termAndId = dummies_[partId]->committedId();
    CHECK_EQ(0, termAndId.first);
    CHECK_EQ(0, termAndId.second);
  }

  // listener will try to pull snapshot for now. Since we have limit the snapshot send rate to 40Kb
  // in 1 second and batch size will be 10Kb, it would take at least 10 second to finish. Besides,
  // there be at least 40 batches
  auto startTime = time::WallClock::fastNowInSec();

  while (true) {
    std::vector<bool> partResult;
    for (int32_t partId = 1; partId <= partCount_; partId++) {
      auto result = dummies_[partId]->committedSnapshot();
      if (result.first >= 10000) {
        partResult.emplace_back(true);
        ASSERT_EQ(10000, dummies_[partId]->data().size());
        ASSERT_GE(time::WallClock::fastNowInSec() - startTime, 10);
        ASSERT_GE(dummies_[partId]->snapshotBatchCount(), 40);
      }
    }
    if (static_cast<int32_t>(partResult.size()) == partCount_) {
      break;
    }
    if (time::WallClock::fastNowInSec() - startTime > 30) {
      ASSERT("It takes to long to pull snapshot");
    }
    sleep(1);
  }
}

INSTANTIATE_TEST_SUITE_P(PartCount_Replicas_ListenerCount,
                         ListenerBasicTest,
                         ::testing::Values(std::make_tuple(1, 1, 1)));

INSTANTIATE_TEST_SUITE_P(PartCount_Replicas_ListenerCount,
                         ListenerAdvanceTest,
                         ::testing::Values(std::make_tuple(1, 1, 1)));

INSTANTIATE_TEST_SUITE_P(PartCount_Replicas_ListenerCount,
                         ListenerSnapshotTest,
                         ::testing::Values(std::make_tuple(1, 1, 1)));

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
