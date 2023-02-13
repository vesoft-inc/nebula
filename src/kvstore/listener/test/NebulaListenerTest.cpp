/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include <map>

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
                std::shared_ptr<folly::Executor> handlers)
      : Listener(spaceId, partId, localAddr, walPath, ioPool, workers, handlers) {}

  std::vector<KV> data() {
    std::vector<KV> ret;
    for (auto& [key, value] : data_) {
      ret.emplace_back(key, value);
    }
    return ret;
  }

  std::tuple<cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(const std::vector<std::string>& rows,
                                                               LogID committedLogId,
                                                               TermID committedLogTerm,
                                                               bool finished) override {
    bool unl = raftLock_.try_lock();
    VLOG(2) << idStr_ << "Listener is committing snapshot.";
    int64_t count = 0;
    int64_t size = 0;
    std::tuple<nebula::cpp2::ErrorCode, nebula::LogID, nebula::TermID> result{
        nebula::cpp2::ErrorCode::SUCCEEDED, count, size};
    BatchHolder batch;
    for (const auto& row : rows) {
      count++;
      size += row.size();
      auto kv = decodeKV(row);
      batch.put(kv.first.toString(), kv.second.toString());
    }
    if (!apply(batch)) {
      LOG(INFO) << idStr_ << "Failed to apply data while committing snapshot.";
      result = {nebula::cpp2::ErrorCode::E_RAFT_PERSIST_SNAPSHOT_FAILED,
                kNoSnapshotCount,
                kNoSnapshotSize};
    } else {
      if (finished) {
        CHECK(!raftLock_.try_lock());
        leaderCommitId_ = committedLogId;
        lastApplyLogId_ = committedLogId;
        persist(committedLogId, committedLogTerm, lastApplyLogId_);
        LOG(INFO) << folly::sformat(
            "Commit snapshot to : committedLogId={},"
            "committedLogTerm={}, lastApplyLogId_={}",
            committedLogId,
            committedLogTerm,
            lastApplyLogId_);
      }
      result = {nebula::cpp2::ErrorCode::SUCCEEDED, count, size};
    }
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

  void processLogs() override {
    std::unique_ptr<LogIterator> iter;
    {
      std::lock_guard<std::mutex> guard(raftLock_);
      if (lastApplyLogId_ >= committedLogId_) {
        return;
      }
      iter = wal_->iterator(lastApplyLogId_ + 1, committedLogId_);
    }

    LogID lastApplyId = -1;
    // // the kv pair which can sync to remote safely
    BatchHolder batch;
    while (iter->valid()) {
      lastApplyId = iter->logId();

      auto log = iter->logMsg();
      if (log.empty()) {
        // skip the heartbeat
        ++(*iter);
        continue;
      }

      DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
      switch (log[sizeof(int64_t)]) {
        case OP_PUT: {
          auto pieces = decodeMultiValues(log);
          DCHECK_EQ(2, pieces.size());
          batch.put(pieces[0].toString(), pieces[1].toString());
          break;
        }
        case OP_MULTI_PUT: {
          auto kvs = decodeMultiValues(log);
          DCHECK_EQ(0, kvs.size() % 2);
          for (size_t i = 0; i < kvs.size(); i += 2) {
            batch.put(kvs[i].toString(), kvs[i + 1].toString());
          }
          break;
        }
        case OP_REMOVE: {
          auto key = decodeSingleValue(log);
          batch.remove(key.toString());
          break;
        }
        case OP_REMOVE_RANGE: {
          auto kvs = decodeMultiValues(log);
          DCHECK_EQ(2, kvs.size());
          batch.rangeRemove(kvs[0].toString(), kvs[1].toString());
          break;
        }
        case OP_MULTI_REMOVE: {
          auto keys = decodeMultiValues(log);
          for (auto key : keys) {
            batch.remove(key.toString());
          }
          break;
        }
        case OP_BATCH_WRITE: {
          auto batchData = decodeBatchValue(log);
          for (auto& op : batchData) {
            // OP_BATCH_REMOVE and OP_BATCH_REMOVE_RANGE is ignored
            switch (op.first) {
              case BatchLogType::OP_BATCH_PUT: {
                batch.put(op.second.first.toString(), op.second.second.toString());
                break;
              }
              case BatchLogType::OP_BATCH_REMOVE: {
                batch.remove(op.second.first.toString());
                break;
              }
              case BatchLogType::OP_BATCH_REMOVE_RANGE: {
                batch.rangeRemove(op.second.first.toString(), op.second.second.toString());
                break;
              }
            }
          }
          break;
        }
        case OP_TRANS_LEADER:
        case OP_ADD_LEARNER:
        case OP_ADD_PEER:
        case OP_REMOVE_PEER: {
          break;
        }
        default: {
          VLOG(2) << idStr_ << "Unknown operation: " << static_cast<int32_t>(log[0]);
        }
      }
      ++(*iter);
    }
    // apply to state machine
    if (lastApplyId != -1 && apply(batch)) {
      std::lock_guard<std::mutex> guard(raftLock_);
      lastApplyLogId_ = lastApplyId;
      persist(committedLogId_, term_, lastApplyLogId_);
      VLOG(2) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
    }
  }

 protected:
  void init() override {}

  bool apply(const BatchHolder& batch) {
    for (auto& log : batch.getBatch()) {
      switch (std::get<0>(log)) {
        case BatchLogType::OP_BATCH_PUT: {
          data_[std::get<1>(log)] = std::get<2>(log);
          break;
        }
        case BatchLogType::OP_BATCH_REMOVE: {
          data_.erase(std::get<1>(log));
          break;
        }
        case BatchLogType::OP_BATCH_REMOVE_RANGE: {
          auto iter = data_.lower_bound(std::get<1>(log));
          while (iter != data_.end()) {
            if (iter->first < std::get<2>(log)) {
              iter = data_.erase(iter);
            } else {
              break;
            }
          }
        }
      }
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
  std::map<std::string, std::string> data_;
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
                                                   listeners_[index]->workers_);
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
    std::map<std::string, std::string> expect;
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
      expect[fmt::format("key_{}_{}", partId, i)] = fmt::format("val_{}_{}", partId, i);
    }
    auto iter = expect.begin();
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++, iter++) {
      CHECK_EQ(iter->first, data[i].first);
      CHECK_EQ(iter->second, data[i].second);
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
    std::map<std::string, std::string> expect;
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
      expect[fmt::format("key_{}_{}", partId, i)] = fmt::format("val_{}_{}", partId, i);
    }
    auto iter = expect.begin();
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++, iter++) {
      CHECK_EQ(iter->first, data[i].first);
      CHECK_EQ(iter->second, data[i].second);
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
    std::map<std::string, std::string> expect;
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
      expect[fmt::format("key_{}_{}", partId, i)] = fmt::format("val_{}_{}", partId, i);
    }
    auto iter = expect.begin();
    for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++, iter++) {
      CHECK_EQ(iter->first, data[i].first);
      CHECK_EQ(iter->second, data[i].second);
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
