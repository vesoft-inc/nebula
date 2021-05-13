/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/fs/FileUtils.h"
#include "common/network/NetworkUtils.h"
#include "common/meta/Common.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include <gtest/gtest.h>
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/LogEncoder.h"
#include "meta/ActiveHostsMan.h"

DECLARE_uint32(raft_heartbeat_interval_secs);
using nebula::meta::PartHosts;
using nebula::meta::ListenerHosts;

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
        : Listener(spaceId, partId, localAddr, walPath,
                   ioPool, workers, handlers, nullptr, nullptr, schemaMan) {
    }

    std::vector<KV> data() {
        return data_;
    }

protected:
    void init() override {
    }

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
        return std::make_pair(committedLogId_, term_);
    }

    LogID lastApplyLogId() override {
        return lastApplyLogId_;
    }

    void cleanup() override {
    }

private:
    std::vector<KV> data_;
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
            listeners_.back()->spaceListeners_.emplace(spaceId_,
                                                       std::make_shared<SpaceListenerInfo>());
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
        return std::make_unique<NebulaStore>(std::move(options),
                                             ioThreadPool,
                                             local,
                                             getWorkers());
    }

    std::unique_ptr<NebulaStore> initListener(int32_t index) {
        auto partMan = std::make_unique<MemPartManager>();
        auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

        KVOptions options;
        options.listenerPath_ = folly::stringPrintf("%s/listener%d", rootPath_->path(), index);
        options.partMan_ = std::move(partMan);
        HostAddr local = listenerHosts_[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             ioThreadPool,
                                             local,
                                             getWorkers());
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
            std::transform(peers_.begin(), peers_.end(), std::back_inserter(raftPeers),
                        [] (const auto& host) {
                return NebulaStore::getRaftAddr(host);
            });
            dummy->start(std::move(raftPeers));
            listeners_[index]->spaceListeners_[spaceId_]->listeners_[partId].emplace(
                meta::cpp2::ListenerType::UNKNOWN, dummy);
            dummys_.emplace(partId, dummy);
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
    // dummys_ is a copy of Listener in listeners_, for convience to check consensus
    std::unordered_map<PartitionID, std::shared_ptr<DummyListener>> dummys_;
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
        stores_[index]->asyncMultiPut(spaceId_, partId, std::move(data),
                                      [&baton](cpp2::ErrorCode code) {
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
        });
        baton.wait();
    }

    // wait listener commit
    sleep(FLAGS_raft_heartbeat_interval_secs);

    LOG(INFO) << "Check listener's data";
    for (int32_t partId = 1; partId <= partCount_; partId++) {
        auto dummy = dummys_[partId];
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
        stores_[index]->asyncMultiPut(spaceId_, partId, std::move(data),
                                      [&baton](cpp2::ErrorCode code) {
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
        });
        baton.wait();
    }

    LOG(INFO) << "Trasfer all part leader to first replica";
    auto targetAddr = NebulaStore::getRaftAddr(peers_[0]);
    for (int32_t partId = 1; partId <= partCount_; partId++) {
        folly::Baton<true, std::atomic> baton;
        auto leader = findLeader(partId);
        auto index = findStoreIndex(leader);
        auto partRet = stores_[index]->part(spaceId_, partId);
        CHECK(ok(partRet));
        auto part = value(partRet);
        part->asyncTransferLeader(targetAddr, [&] (cpp2::ErrorCode) {
            baton.post();
        });
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
        stores_[index]->asyncMultiPut(spaceId_, partId, std::move(data),
                                      [&baton](cpp2::ErrorCode code) {
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
        });
        baton.wait();
    }

    // wait listener commit
    sleep(FLAGS_raft_heartbeat_interval_secs);

    LOG(INFO) << "Check listener's data";
    for (int32_t partId = 1; partId <= partCount_; partId++) {
        auto dummy = dummys_[partId];
        const auto& data = dummy->data();
        CHECK_EQ(200, data.size());
        for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
            CHECK_EQ(folly::stringPrintf("key_%d_%d", partId, i), data[i].first);
            CHECK_EQ(folly::stringPrintf("val_%d_%d", partId, i), data[i].second);
        }
    }
}

INSTANTIATE_TEST_CASE_P(
    PartCount_Replicas_ListenerCount,
    ListenerBasicTest,
    ::testing::Values(std::make_tuple(1, 1, 1)));

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
