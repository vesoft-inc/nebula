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
#include "kvstore/NebulaStore.h"
#include "kvstore/RocksEngine.h"
#include "kvstore/plugins/topology/TopoListener.h"
#include "meta/ActiveHostsMan.h"

DECLARE_uint32(raft_heartbeat_interval_secs);

using nebula::meta::PartHosts;

namespace nebula {
namespace kvstore {

class TopoListenerTest : public ::testing::TestWithParam<std::tuple<int32_t, int32_t, int32_t>> {
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
      auto listenerStore = initListener(i);
      listenerStore->init();
      listenerStore->spaceListeners_[spaceId_] = std::make_shared<SpaceListenerInfo>();
      listenerStore->spaceListeners_[spaceId_]->engine_ =
          std::make_unique<RocksEngine>(spaceId_, vIdLen_, listenerStore->options_.listenerPath_);
      listeners_.emplace_back(std::move(listenerStore));
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
            listeners[partId % listeners.size()], meta::cpp2::ListenerType::GRAPH_TOPOLOGY);
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
          "%s/listener%lu/nebula/%d/wal/%d", rootPath_->path(), index, spaceId_, partId);
      auto local = NebulaStore::getRaftAddr(listenerHosts_[index]);
      auto dummy = std::make_shared<TopoListener>(
          spaceId_,
          partId,
          local,
          walPath,
          listeners_[index]->ioPool_,
          listeners_[index]->bgWorkers_,
          listeners_[index]->workers_,
          listeners_[index]->spaceListeners_[spaceId_]->engine_.get(),
          vIdLen_);
      listeners_[index]->raftService_->addPartition(dummy);
      std::vector<HostAddr> raftPeers;
      std::transform(
          peers_.begin(), peers_.end(), std::back_inserter(raftPeers), [](const auto& host) {
            return NebulaStore::getRaftAddr(host);
          });
      dummy->start(std::move(raftPeers));
      listeners_[index]->spaceListeners_[spaceId_]->listeners_[partId].emplace(
          meta::cpp2::ListenerType::GRAPH_TOPOLOGY, dummy);
      dummies_.emplace(partId, dummy);
    }
  }

  std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> getWorkers() {
    auto worker =
        apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1, true);
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
  int32_t vIdLen_ = 8;
  std::vector<HostAddr> peers_;
  std::vector<HostAddr> listenerHosts_;
  std::vector<std::unique_ptr<NebulaStore>> stores_;
  std::vector<std::unique_ptr<NebulaStore>> listeners_;
  // dummies_ is a copy of Listener in listeners_, for convenience to check consensus
  std::unordered_map<PartitionID, std::shared_ptr<TopoListener>> dummies_;
  std::unique_ptr<KVEngine> engine_;
};

TEST_P(TopoListenerTest, SimpleTest) {
  LOG(INFO) << "Insert some data";
  EdgeRanking rank = 0;
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    // each partition has 10 tags, 10 edges
    std::vector<KV> data;
    VertexID vId = folly::stringPrintf("id-%d", partId);
    for (TagID tagId = 0; tagId < 10; tagId++) {
      data.emplace_back(NebulaKeyUtils::tagKey(vIdLen_, partId, vId, tagId),
                        folly::stringPrintf("tag_%d", tagId));
    }
    for (EdgeType edgeType = 0; edgeType < 10; edgeType++) {
      data.emplace_back(NebulaKeyUtils::edgeKey(vIdLen_, partId, vId, edgeType, rank, vId),
                        folly::stringPrintf("edge_%d", edgeType));
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
  // all data will only write to one part, which is TopoListener::kTopoPartId
  PartitionID listenerPart = 0;
  for (int32_t i = 0; i < listenerCount_; i++) {
    auto listener = listeners_[i].get();
    // part prefix
    {
      std::unique_ptr<kvstore::KVIterator> iter;
      // the listenerPart is only used to build prefix
      auto tagPrefix = NebulaKeyUtils::tagPrefix(listenerPart);
      auto code = listener->prefix(spaceId_, listenerPart, tagPrefix, &iter, true);
      EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
      int32_t count = 0;
      while (iter->valid()) {
        EXPECT_EQ(iter->val(), "");
        iter->next();
        count++;
      }
      EXPECT_EQ(count, partCount_ * 10);
    }
    {
      std::unique_ptr<kvstore::KVIterator> iter;
      // the listenerPart is only used to build prefix
      auto edgePrefix = NebulaKeyUtils::edgePrefix(listenerPart);
      auto code = listener->prefix(spaceId_, listenerPart, edgePrefix, &iter, true);
      EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
      int32_t count = 0;
      while (iter->valid()) {
        EXPECT_EQ(iter->val(), "");
        iter->next();
        count++;
      }
      EXPECT_EQ(count, partCount_ * 10);
    }
    // vId prefix
    {
      std::unique_ptr<kvstore::KVIterator> iter;
      // partId is only use to build vid prefix
      for (int32_t partId = 1; partId <= partCount_; partId++) {
        VertexID vId = folly::stringPrintf("id-%d", partId);
        auto tagPrefix = NebulaKeyUtils::tagPrefix(vIdLen_, listenerPart, vId);
        // the listenerPart is only used to build prefix
        auto code = listener->prefix(spaceId_, listenerPart, tagPrefix, &iter, true);
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        int32_t count = 0;
        while (iter->valid()) {
          EXPECT_EQ(iter->val(), "");
          iter->next();
          count++;
        }
        EXPECT_EQ(count, 10);
      }
    }
    // vId prefix
    {
      std::unique_ptr<kvstore::KVIterator> iter;
      // partId is only use to build vid prefix
      for (int32_t partId = 1; partId <= partCount_; partId++) {
        VertexID vId = folly::stringPrintf("id-%d", partId);
        auto edgePrefix = NebulaKeyUtils::edgePrefix(vIdLen_, listenerPart, vId);
        // the listenerPart is only used to build prefix
        auto code = listener->prefix(spaceId_, listenerPart, edgePrefix, &iter, true);
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        int32_t count = 0;
        while (iter->valid()) {
          EXPECT_EQ(iter->val(), "");
          iter->next();
          count++;
        }
        EXPECT_EQ(count, 10);
      }
    }
  }
}

INSTANTIATE_TEST_CASE_P(PartCount_Replicas_ListenerCount,
                        TopoListenerTest,
                        ::testing::Values(std::make_tuple(1, 1, 1),
                                          std::make_tuple(10, 1, 1),
                                          std::make_tuple(10, 3, 1)));

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
