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
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <iostream>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/RocksEngine.h"
#include "kvstore/RocksEngineConfig.h"
#include "kvstore/LogEncoder.h"
#include "meta/ActiveHostsMan.h"

DECLARE_uint32(raft_heartbeat_interval_secs);
DECLARE_bool(auto_remove_invalid_space);
const int32_t kDefaultVidLen = 8;
using nebula::meta::PartHosts;

namespace nebula {
namespace kvstore {

template<typename T>
void dump(const std::vector<T>& v) {
    std::stringstream ss;
    for (auto& e : v) {
        ss << e << ", ";
    }
    VLOG(1) << ss.str();
}

std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager>
getHandlers() {
    auto handlersPool
        = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                                 1, true /*stats*/);
    handlersPool->setNamePrefix("executor");
    handlersPool->start();
    return handlersPool;
}

TEST(NebulaStoreTest, SimpleTest) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    // GraphSpaceID =>  {PartitionIDs}
    // 1 => {0, 1, 2, 3, 4, 5}
    // 2 => {0, 1, 2, 3, 4, 5}
    for (auto spaceId = 1; spaceId <=2; spaceId++) {
        for (auto partId = 0; partId < 6; partId++) {
            partMan->partsMap_[spaceId][partId] = PartHosts();
        }
    }

    VLOG(1) << "Total space num is " << partMan->partsMap_.size()
            << ", total local partitions num is "
            << partMan->parts(HostAddr("", 0)).size();

    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath.path()));
    paths.emplace_back(folly::stringPrintf("%s/disk2", rootPath.path()));

    KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    HostAddr local = {"", 0};
    auto store = std::make_unique<NebulaStore>(std::move(options),
                                               ioThreadPool,
                                               local,
                                               getHandlers());
    store->init();
    sleep(1);
    EXPECT_EQ(2, store->spaces_.size());

    EXPECT_EQ(6, store->spaces_[1]->parts_.size());
    EXPECT_EQ(2, store->spaces_[1]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1/nebula/1", rootPath.path()),
              store->spaces_[1]->engines_[0]->getDataRoot());
    EXPECT_EQ(folly::stringPrintf("%s/disk2/nebula/1", rootPath.path()),
              store->spaces_[1]->engines_[1]->getDataRoot());

    EXPECT_EQ(6, store->spaces_[2]->parts_.size());
    EXPECT_EQ(2, store->spaces_[2]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1/nebula/2", rootPath.path()),
              store->spaces_[2]->engines_[0]->getDataRoot());
    EXPECT_EQ(folly::stringPrintf("%s/disk2/nebula/2", rootPath.path()),
              store->spaces_[2]->engines_[1]->getDataRoot());

    store->asyncMultiPut(0, 0, {{"key", "val"}}, [](nebula::cpp2::ErrorCode code) {
        EXPECT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, code);
    });

    store->asyncMultiPut(1, 6, {{"key", "val"}}, [](nebula::cpp2::ErrorCode code) {
        EXPECT_EQ(nebula::cpp2::ErrorCode::E_PART_NOT_FOUND, code);
    });

    VLOG(1) << "Put some data then read them...";

    std::string prefix = "prefix";
    std::vector<KV> data;
    for (auto i = 0; i < 100; i++) {
        data.emplace_back(prefix + std::string(reinterpret_cast<const char*>(&i),
                                               sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    folly::Baton<true, std::atomic> baton;
    store->asyncMultiPut(1, 1, std::move(data), [&] (nebula::cpp2::ErrorCode code) {
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        baton.post();
    });
    baton.wait();
    int32_t start = 0;
    int32_t end = 100;
    std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
    std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
    s = prefix + s;
    e = prefix + e;
    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, store->range(1, 1, s, e, &iter));
    int num = 0;
    auto prefixLen = prefix.size();
    while (iter->valid()) {
        auto key = *reinterpret_cast<const int32_t*>(iter->key().data() + prefixLen);
        auto val = iter->val();
        EXPECT_EQ(num, key);
        EXPECT_EQ(folly::stringPrintf("val_%d", num), val);
        iter->next();
        num++;
    }
    EXPECT_EQ(100, num);
}

TEST(NebulaStoreTest, PartsTest) {
    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto partMan = std::make_unique<MemPartManager>();

    // GraphSpaceID =>  {PartitionIDs}
    // 0 => {0, 1, 2, 3...9}
    // The parts on PartMan is 0...9
    for (auto partId = 0; partId < 10; partId++) {
        partMan->addPart(0, partId);
    }

    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath.path()));
    paths.emplace_back(folly::stringPrintf("%s/disk2", rootPath.path()));

    for (size_t i = 0; i < paths.size(); i++) {
        auto db = std::make_unique<RocksEngine>(
            0, /* spaceId */
            kDefaultVidLen,
            paths[i]);
        for (auto partId = 0; partId < 3; partId++) {
            db->addPart(5 * i + partId);
        }
        db->addPart(5 * i + 10);
        auto parts = db->allParts();
        dump(parts);
    }
    // Currently, the disks hold parts as below:
    // disk1: 0, 1, 2, 10
    // disk2: 5, 6, 7, 15

    KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    HostAddr local = {"", 0};
    auto store = std::make_unique<NebulaStore>(std::move(options),
                                               ioThreadPool,
                                               local,
                                               getHandlers());
    store->init();
    auto check = [&](GraphSpaceID spaceId) {
        for (auto i = 0; i < 2; i++) {
            ASSERT_EQ(folly::stringPrintf("%s/disk%d/nebula/%d",
                                          rootPath.path(),
                                          i + 1,
                                          spaceId),
                      store->spaces_[spaceId]->engines_[i]->getDataRoot());
            auto parts = store->spaces_[spaceId]->engines_[i]->allParts();
            dump(parts);
            ASSERT_EQ(5, parts.size());
        }
    };
    check(0);
    // After init, the parts should be 0-9, and the distribution should be
    // disk1: 0, 1, 2, 3, 8
    // disk2: 4, 5, 6, 7, 9
    // After restart, the original order should not been broken.
    {
        auto parts = store->spaces_[0]->engines_[0]->allParts();
        ASSERT_EQ(0, parts[0]);
        ASSERT_EQ(1, parts[1]);
        ASSERT_EQ(2, parts[2]);
        ASSERT_EQ(3, parts[3]);
        ASSERT_EQ(8, parts[4]);
    }
    {
        auto parts = store->spaces_[0]->engines_[1]->allParts();
        ASSERT_EQ(4, parts[0]);
        ASSERT_EQ(5, parts[1]);
        ASSERT_EQ(6, parts[2]);
        ASSERT_EQ(7, parts[3]);
        ASSERT_EQ(9, parts[4]);
    }

    auto* pm = dynamic_cast<MemPartManager*>(store->options_.partMan_.get());
    // Let's create another space with 10 parts.
    for (auto partId = 0; partId < 10; partId++) {
        pm->addPart(1, partId);
    }
    check(1);
    {
        auto parts = store->spaces_[1]->engines_[0]->allParts();
        ASSERT_EQ(0, parts[0]);
        ASSERT_EQ(2, parts[1]);
        ASSERT_EQ(4, parts[2]);
        ASSERT_EQ(6, parts[3]);
        ASSERT_EQ(8, parts[4]);
    }
    {
        auto parts = store->spaces_[1]->engines_[1]->allParts();
        ASSERT_EQ(1, parts[0]);
        ASSERT_EQ(3, parts[1]);
        ASSERT_EQ(5, parts[2]);
        ASSERT_EQ(7, parts[3]);
        ASSERT_EQ(9, parts[4]);
    }

    // Let's remove space some parts in GraphSpace 0
    for (auto partId = 0; partId < 5; partId++) {
        pm->removePart(0, partId);
    }

    int32_t totalParts = 0;
    for (auto i = 0; i < 2; i++) {
        ASSERT_EQ(folly::stringPrintf("%s/disk%d/nebula/0", rootPath.path(), i + 1),
                  store->spaces_[0]->engines_[i]->getDataRoot());
        auto parts = store->spaces_[0]->engines_[i]->allParts();
        dump(parts);
        totalParts += parts.size();
    }
    ASSERT_EQ(5, totalParts);

    for (auto partId = 5; partId < 10; partId++) {
        pm->removePart(0, partId);
    }
    ASSERT_TRUE(store->spaces_.find(0) == store->spaces_.end());
}

TEST(NebulaStoreTest, ThreeCopiesTest) {
    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    auto initNebulaStore = [](const std::vector<HostAddr>& peers,
                              int32_t index,
                              const std::string& path) -> std::unique_ptr<NebulaStore> {
        LOG(INFO) << "Start nebula store on " << peers[index];
        auto sIoThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        auto partMan = std::make_unique<MemPartManager>();
        for (auto partId = 0; partId < 3; partId++) {
            PartHosts pm;
            pm.spaceId_ = 0;
            pm.partId_ = partId;
            pm.hosts_ = peers;
            partMan->partsMap_[0][partId] = std::move(pm);
        }
        std::vector<std::string> paths;
        paths.emplace_back(folly::stringPrintf("%s/disk%d", path.c_str(), index));
        KVOptions options;
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);
        HostAddr local = peers[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             sIoThreadPool,
                                             local,
                                             getHandlers());
    };
    int32_t replicas = 3;
    std::string ip("127.0.0.1");
    std::vector<HostAddr> peers;
    for (int32_t i = 0; i < replicas; i++) {
        peers.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }

    std::vector<std::unique_ptr<NebulaStore>> stores;
    for (int i = 0; i < replicas; i++) {
        stores.emplace_back(initNebulaStore(peers, i, rootPath.path()));
        stores.back()->init();
    }
    LOG(INFO) << "Waiting for all leaders elected!";
    while (true) {
        int32_t leaderCount = 0;
        for (int i = 0; i < replicas; i++) {
            nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
            leaderCount += stores[i]->allLeader(leaderIds);
        }
        if (leaderCount == 3) {
            break;
        }
        usleep(100000);
    }
    LOG(INFO) << "Let's write some logs...";

    auto findStoreIndex = [&] (const HostAddr& addr) {
        for (size_t i = 0; i < peers.size(); i++) {
            if (peers[i] == addr) {
                return i;
            }
        }
        LOG(FATAL) << "Should not reach here!";
        return 0UL;
    };
    LOG(INFO) << "Put some data then read them...";
    for (int part = 0; part < 3; part++) {
        std::string prefix = "prefix";
        std::vector<KV> data;
        for (auto i = 0; i < 100; i++) {
            data.emplace_back(prefix + std::string(reinterpret_cast<const char*>(&i),
                                                   sizeof(int32_t)),
                              folly::stringPrintf("val_%d_%d", part, i));
        }
        auto res = stores[0]->partLeader(0, part);
        CHECK(ok(res));
        auto leader = value(std::move(res));

        auto index = findStoreIndex(leader);
        {
            folly::Baton<true, std::atomic> baton;
            stores[index]->asyncMultiPut(0, part, std::move(data),
                                         [&baton](nebula::cpp2::ErrorCode code) {
                EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                baton.post();
            });
            baton.wait();
        }
        sleep(FLAGS_raft_heartbeat_interval_secs);
        {
            int32_t start = 0;
            int32_t end = 100;
            std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
            std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
            s = prefix + s;
            e = prefix + e;
            for (int i = 0; i < replicas; i++) {
                LOG(INFO) << "Check the data on " << stores[i]->raftAddr_ << " for part " << part;
                auto ret = stores[i]->engine(0, part);
                ASSERT(ok(ret));
                auto* engine = value(std::move(ret));
                std::unique_ptr<KVIterator> iter;
                ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(s, e, &iter));
                int num = 0;
                auto prefixLen = prefix.size();
                while (iter->valid()) {
                    auto key = *reinterpret_cast<const int32_t*>(iter->key().data() + prefixLen);
                    auto val = iter->val();
                    ASSERT_EQ(num, key);
                    ASSERT_EQ(folly::stringPrintf("val_%d_%d", part, num), val);
                    iter->next();
                    num++;
                }
                ASSERT_EQ(100, num);
            }
        }
        // Let's try to write data on follower;
        auto followerIndex = (index + 1) % replicas;
        {
            folly::Baton<true, std::atomic> baton;
            stores[followerIndex]->asyncMultiPut(0,
                                                 part,
                                                 {{"key", "val"}},
                                                 [&baton](nebula::cpp2::ErrorCode code) {
                EXPECT_EQ(nebula::cpp2::ErrorCode::E_LEADER_CHANGED, code);
                baton.post();
            });
            baton.wait();
        }
        // Let's try to read data on follower
        {
            std::string value;
            auto ret = stores[followerIndex]->get(0, part, "key", &value);
            EXPECT_EQ(nebula::cpp2::ErrorCode::E_LEADER_CHANGED, ret);
        }
    }
}

TEST(NebulaStoreTest, TransLeaderTest) {
    fs::TempDir rootPath("/tmp/trans_leader_test.XXXXXX");
    auto initNebulaStore = [](const std::vector<HostAddr>& peers,
                              int32_t index,
                              const std::string& path) -> std::unique_ptr<NebulaStore> {
        LOG(INFO) << "Start nebula store on " << peers[index];
        auto sIoThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        auto partMan = std::make_unique<MemPartManager>();
        for (auto partId = 0; partId < 3; partId++) {
            PartHosts pm;
            pm.spaceId_ = 0;
            pm.partId_ = partId;
            pm.hosts_ = peers;
            partMan->partsMap_[0][partId] = std::move(pm);
        }
        std::vector<std::string> paths;
        paths.emplace_back(folly::stringPrintf("%s/disk%d", path.c_str(), index));
        KVOptions options;
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);
        HostAddr local = peers[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             sIoThreadPool,
                                             local,
                                             getHandlers());
    };

    // 3 replicas, 3 partition
    int32_t replicas = 3;
    std::string ip("127.0.0.1");
    std::vector<HostAddr> peers;
    for (int32_t i = 0; i < replicas; i++) {
        peers.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }

    std::vector<std::unique_ptr<NebulaStore>> stores;
    for (int i = 0; i < replicas; i++) {
        stores.emplace_back(initNebulaStore(peers, i, rootPath.path()));
        stores.back()->init();
    }
    sleep(FLAGS_raft_heartbeat_interval_secs);
    LOG(INFO) << "Waiting for all leaders elected!";
    while (true) {
        int32_t leaderCount = 0;
        for (int i = 0; i < replicas; i++) {
            nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
            leaderCount += stores[i]->allLeader(leaderIds);
        }
        if (leaderCount == 3) {
            break;
        }
        usleep(100000);
    }

    auto findStoreIndex = [&] (const HostAddr& addr) {
        for (size_t i = 0; i < peers.size(); i++) {
            if (peers[i] == addr) {
                return i;
            }
        }
        LOG(FATAL) << "Should not reach here!";
        return 0UL;
    };

    LOG(INFO) << "Transfer leader to first copy";
    // all parttition tranfer leaders to first replica
    GraphSpaceID spaceId = 0;
    for (int i = 0; i < 3; i++) {
        PartitionID partId = i;
        auto targetAddr = NebulaStore::getRaftAddr(peers[0]);
        folly::Baton<true, std::atomic> baton;
        LOG(INFO) << "try to trans leader to " << targetAddr;

        auto leaderRet = stores[0]->partLeader(spaceId, partId);
        CHECK(ok(leaderRet));
        auto leader = value(std::move(leaderRet));
        auto index = findStoreIndex(leader);
        auto partRet = stores[index]->part(spaceId, partId);
        CHECK(ok(partRet));
        auto part = value(partRet);
        part->asyncTransferLeader(targetAddr, [&] (nebula::cpp2::ErrorCode) {
            baton.post();
        });
        baton.wait();
    }
    sleep(FLAGS_raft_heartbeat_interval_secs);
    {
        nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
        ASSERT_EQ(3, stores[0]->allLeader(leaderIds));
    }

    LOG(INFO) << "Manual leader balance";
    // stores[0] transfer leader to other replica, each one have a leader
    // leader of parts would be {0: peers[0], 1: peers[1], 2: peers[2]}
    for (int i = 0; i < 3; i++) {
        PartitionID partId = i;
        auto targetAddr = NebulaStore::getRaftAddr(peers[i]);
        folly::Baton<true, std::atomic> baton;
        auto ret = stores[0]->part(spaceId, partId);
        CHECK(ok(ret));
        auto part = nebula::value(ret);
        LOG(INFO) << "Transfer part " << partId << " leader to " << targetAddr;
        part->asyncTransferLeader(targetAddr, [&] (nebula::cpp2::ErrorCode) {
            baton.post();
        });
        baton.wait();
    }
    sleep(FLAGS_raft_heartbeat_interval_secs);
    for (int i = 0; i < replicas; i++) {
        nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
        ASSERT_EQ(1UL, stores[i]->allLeader(leaderIds));
    }
}

TEST(NebulaStoreTest, CheckpointTest) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    // GraphSpaceID =>  {PartitionIDs}
    // 1 => {0, 1, 2, 3, 4, 5}
    // 2 => {0, 1, 2, 3, 4, 5}
    for (auto spaceId = 1; spaceId <=2; spaceId++) {
        for (auto partId = 0; partId < 6; partId++) {
            partMan->partsMap_[spaceId][partId] = PartHosts();
        }
    }

    VLOG(1) << "Total space num is " << partMan->partsMap_.size()
            << ", total local partitions num is "
            << partMan->parts(HostAddr("", 0)).size();

    fs::TempDir srcPath("/tmp/nebula_checkpoint_src_test.XXXXXX");
    std::vector<std::string> spaths;
    spaths.emplace_back(folly::stringPrintf("%s/disk1", srcPath.path()));
    spaths.emplace_back(folly::stringPrintf("%s/disk2", srcPath.path()));

    KVOptions options;
    options.dataPaths_ = std::move(spaths);
    options.partMan_ = std::move(partMan);
    HostAddr local = {"", 0};
    auto store = std::make_unique<NebulaStore>(std::move(options),
                                               ioThreadPool,
                                               local,
                                               getHandlers());
    store->init();
    sleep(1);
    EXPECT_EQ(2, store->spaces_.size());

    EXPECT_EQ(6, store->spaces_[1]->parts_.size());
    EXPECT_EQ(2, store->spaces_[1]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1/nebula/1", srcPath.path()),
              store->spaces_[1]->engines_[0]->getDataRoot());
    EXPECT_EQ(folly::stringPrintf("%s/disk2/nebula/1", srcPath.path()),
              store->spaces_[1]->engines_[1]->getDataRoot());

    EXPECT_EQ(6, store->spaces_[2]->parts_.size());
    EXPECT_EQ(2, store->spaces_[2]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1/nebula/2", srcPath.path()),
              store->spaces_[2]->engines_[0]->getDataRoot());
    EXPECT_EQ(folly::stringPrintf("%s/disk2/nebula/2", srcPath.path()),
              store->spaces_[2]->engines_[1]->getDataRoot());

    store->asyncMultiPut(0, 0, {{"key", "val"}}, [](nebula::cpp2::ErrorCode code) {
        EXPECT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, code);
    });

    store->asyncMultiPut(1, 6, {{"key", "val"}}, [](nebula::cpp2::ErrorCode code) {
        EXPECT_EQ(nebula::cpp2::ErrorCode::E_PART_NOT_FOUND, code);
    });

    VLOG(1) << "Put some data then read them...";

    std::string prefix = "prefix";
    std::vector<KV> data;
    for (auto i = 0; i < 100; i++) {
        data.emplace_back(prefix + std::string(reinterpret_cast<const char*>(&i),
                                               sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    folly::Baton<true, std::atomic> baton;
    store->asyncMultiPut(1, 1, std::move(data), [&] (nebula::cpp2::ErrorCode code) {
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        baton.post();
    });
    baton.wait();

    auto ret = store->createCheckpoint(1, "test_checkpoint");
    ASSERT_TRUE(ret.isRightType());
    ret = store->createCheckpoint(2, "test_checkpoint");
    ASSERT_TRUE(ret.isRightType());
}

TEST(NebulaStoreTest, ThreeCopiesCheckpointTest) {
    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    auto initNebulaStore = [](const std::vector<HostAddr>& peers,
                              int32_t index,
                              const std::string& path) -> std::unique_ptr<NebulaStore> {
        LOG(INFO) << "Start nebula store on " << peers[index];
        auto sIoThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        auto partMan = std::make_unique<MemPartManager>();
        for (auto partId = 0; partId < 3; partId++) {
            PartHosts pm;
            pm.spaceId_ = 0;
            pm.partId_ = partId;
            pm.hosts_ = peers;
            partMan->partsMap_[0][partId] = std::move(pm);
        }
        std::vector<std::string> paths;
        std::string dataDir = folly::stringPrintf("%s/disk%d", path.c_str(), index);
        paths.emplace_back(std::move(dataDir));
        KVOptions options;
        options.dataPaths_ = paths;
        options.partMan_ = std::move(partMan);
        HostAddr local = peers[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             sIoThreadPool,
                                             local,
                                             getHandlers());
    };

    auto waitLeader = [] (std::vector<std::unique_ptr<NebulaStore>>& stores) {
        int replicas = 3;
        while (true) {
            int32_t leaderCount = 0;
            for (int i = 0; i < replicas; i++) {
                nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
                leaderCount += stores[i]->allLeader(leaderIds);
            }
            if (leaderCount == 3) {
                break;
            }
            usleep(100000);
        }
    };

    int32_t replicas = 3;
    std::string ip("127.0.0.1");
    std::vector<HostAddr> peers;
    for (int32_t i = 0; i < replicas; i++) {
        peers.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }

    std::vector<std::unique_ptr<NebulaStore>> stores;
    for (int i = 0; i < replicas; i++) {
        stores.emplace_back(initNebulaStore(peers, i, rootPath.path()));
        stores.back()->init();
    }
    LOG(INFO) << "Waiting for all leaders elected!";
    waitLeader(stores);

    LOG(INFO) << "Let's write some logs...";

    auto findStoreIndex = [&] (const HostAddr& addr) {
        for (size_t i = 0; i < peers.size(); i++) {
            if (peers[i] == addr) {
                return i;
            }
        }
        LOG(FATAL) << "Should not reach here!";
        return 0UL;
    };
    LOG(INFO) << "Put some data then read them...";
    for (int part = 0; part < 3; part++) {
        std::string prefix = "prefix";
        std::vector<KV> data;
        for (auto i = 0; i < 100; i++) {
            data.emplace_back(prefix + std::string(reinterpret_cast<const char*>(&i),
                                                   sizeof(int32_t)),
                              folly::stringPrintf("val_%d_%d", part, i));
        }
        auto res = stores[0]->partLeader(0, part);
        CHECK(ok(res));
        auto leader = value(std::move(res));

        auto index = findStoreIndex(leader);
        {
            folly::Baton<true, std::atomic> baton;
            stores[index]->asyncMultiPut(0, part, std::move(data),
                                         [&baton](nebula::cpp2::ErrorCode code) {
                EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                baton.post();
            });
            baton.wait();
        }
        sleep(FLAGS_raft_heartbeat_interval_secs);
        {
            int32_t start = 0;
            int32_t end = 100;
            std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
            std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
            s = prefix + s;
            e = prefix + e;
            for (int i = 0; i < replicas; i++) {
                LOG(INFO) << "Check the data on " << stores[i]->raftAddr_ << " for part " << part;
                auto ret = stores[i]->engine(0, part);
                ASSERT(ok(ret));
                auto* engine = value(std::move(ret));
                std::unique_ptr<KVIterator> iter;
                ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(s, e, &iter));
                int num = 0;
                auto prefixLen = prefix.size();
                while (iter->valid()) {
                    auto key = *reinterpret_cast<const int32_t*>(iter->key().data() + prefixLen);
                    auto val = iter->val();
                    ASSERT_EQ(num, key);
                    ASSERT_EQ(folly::stringPrintf("val_%d_%d", part, num), val);
                    iter->next();
                    num++;
                }
                ASSERT_EQ(100, num);
            }
        }

        // Let's try to write data on follower;
        auto followerIndex = (index + 1) % replicas;
        {
            folly::Baton<true, std::atomic> baton;
            stores[followerIndex]->asyncMultiPut(0,
                                                 part,
                                                 {{"key", "val"}},
                                                 [&baton](nebula::cpp2::ErrorCode code) {
                EXPECT_EQ(nebula::cpp2::ErrorCode::E_LEADER_CHANGED, code);
                baton.post();
            });
            baton.wait();
        }
    }

    for (int i = 0; i < replicas; i++) {
        auto ret = stores[i]->createCheckpoint(0, "snapshot");
        ASSERT_TRUE(ret.isRightType());
    }

    sleep(FLAGS_raft_heartbeat_interval_secs);

    for (int i = 0; i < replicas; i++) {
        stores[i].reset(nullptr);
    }

    for (int i = 0; i < replicas; i++) {
        std::string rm = folly::stringPrintf("%s/disk%d/nebula/0", rootPath.path(), i);
        fs::FileUtils::remove(folly::stringPrintf("%s/data", rm.data()).c_str(), true);
        fs::FileUtils::remove(folly::stringPrintf("%s/wal", rm.data()).c_str(), true);
        std::string src = folly::stringPrintf(
            "%s/disk%d/nebula/0/checkpoints/snapshot/data",
            rootPath.path(), i);
        std::string dst = folly::stringPrintf(
            "%s/disk%d/nebula/0/data",
            rootPath.path(), i);
        ASSERT_TRUE(fs::FileUtils::rename(src, dst));

        src = folly::stringPrintf(
            "%s/disk%d/nebula/0/checkpoints/snapshot/wal",
            rootPath.path(), i);
        dst = folly::stringPrintf(
            "%s/disk%d/nebula/0/wal",
            rootPath.path(), i);
        ASSERT_TRUE(fs::FileUtils::rename(src, dst));
    }

    LOG(INFO) << "Let's start the engine via checkpoint";
    std::vector<HostAddr> cPeers;
    for (int32_t i = 0; i < replicas; i++) {
        cPeers.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }

    std::vector<std::unique_ptr<NebulaStore>> cStores;
    for (int i = 0; i < replicas; i++) {
        cStores.emplace_back(initNebulaStore(cPeers, i, rootPath.path()));
        cStores.back()->init();
    }

    LOG(INFO) << "Waiting for all leaders elected!";
    waitLeader(cStores);

    LOG(INFO) << "Verify data";
    for (int part = 0; part < 3; part++) {
        std::string prefix = "prefix";
        std::vector<KV> data;
        for (auto i = 0; i < 100; i++) {
            data.emplace_back(
                    prefix + std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                    folly::stringPrintf("val_%d_%d", part, i));
        }
        auto res = cStores[0]->partLeader(0, part);
        CHECK(ok(res));
        sleep(FLAGS_raft_heartbeat_interval_secs);
        {
            int32_t start = 0;
            int32_t end = 100;
            std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
            std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
            s = prefix + s;
            e = prefix + e;
            for (int i = 0; i < replicas; i++) {
                LOG(INFO) << "Check the data on " << cStores[i]->raftAddr_ << " for part " << part;
                auto ret = cStores[i]->engine(0, part);
                ASSERT(ok(ret));
                auto* engine = value(std::move(ret));
                std::unique_ptr<KVIterator> iter;
                ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(s, e, &iter));
                int num = 0;
                while (iter->valid()) {
                    iter->next();
                    num++;
                }
                ASSERT_EQ(100, num);
            }
        }
    }
}

TEST(NebulaStoreTest, AtomicOpBatchTest) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    // space id : 1 , part id : 0
    partMan->partsMap_[1][0] = PartHosts();

    VLOG(1) << "Total space num is " << partMan->partsMap_.size()
            << ", total local partitions num is "
            << partMan->parts(HostAddr("", 0)).size();

    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath.path()));

    KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    HostAddr local = {"", 0};
    auto store = std::make_unique<NebulaStore>(std::move(options),
                                               ioThreadPool,
                                               local,
                                               getHandlers());
    store->init();
    sleep(FLAGS_raft_heartbeat_interval_secs);
    // put kv
    {
        std::vector<std::pair<std::string, std::string>> expected, result;
        auto atomic = [&]() -> std::string {
            std::unique_ptr<kvstore::BatchHolder> batchHolder =
                    std::make_unique<kvstore::BatchHolder>();
            for (auto i = 0; i < 20; i++) {
                auto key = folly::stringPrintf("key_%d", i);
                auto val = folly::stringPrintf("val_%d", i);
                batchHolder->put(key.data(), val.data());
                expected.emplace_back(std::move(key), std::move(val));
            }
            return encodeBatchValue(batchHolder->getBatch());
        };

        folly::Baton<true, std::atomic> baton;
        auto callback = [&] (nebula::cpp2::ErrorCode code) {
            EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
        };
        store->asyncAtomicOp(1, 0, atomic, callback);
        baton.wait();
        std::unique_ptr<kvstore::KVIterator> iter;
        std::string prefix("key");
        auto ret = store->prefix(1, 0, prefix, &iter);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
        while (iter->valid()) {
            result.emplace_back(iter->key(), iter->val());
            iter->next();
        }
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, result);
    }
    // put and remove
    {
        std::vector<std::pair<std::string, std::string>> expected, result;
        auto atomic = [&]() -> std::string {
            std::unique_ptr<kvstore::BatchHolder> batchHolder =
                    std::make_unique<kvstore::BatchHolder>();
            for (auto i = 0; i < 20; i++) {
                auto key = folly::stringPrintf("key_%d", i);
                auto val = folly::stringPrintf("val_%d", i);
                batchHolder->put(key.data(), val.data());
                if (i%5 != 0) {
                    expected.emplace_back(std::move(key), std::move(val));
                }
            }
            for (auto i = 0; i < 20; i = i + 5) {
                batchHolder->remove(folly::stringPrintf("key_%d", i));
            }
            return encodeBatchValue(batchHolder->getBatch());
        };

        folly::Baton<true, std::atomic> baton;
        auto callback = [&] (nebula::cpp2::ErrorCode code) {
            EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
        };
        store->asyncAtomicOp(1, 0, atomic, callback);
        baton.wait();
        std::unique_ptr<kvstore::KVIterator> iter;
        std::string prefix("key");
        auto ret = store->prefix(1, 0, prefix, &iter);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
        while (iter->valid()) {
            result.emplace_back(iter->key(), iter->val());
            iter->next();
        }
        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(expected, result);
    }
}

TEST(NebulaStoreTest, RemoveInvalidSpaceTest) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    // GraphSpaceID =>  {PartitionIDs}
    // 1 => {0, 1, 2, 3, 4, 5}
    // 2 => {0, 1, 2, 3, 4, 5}
    for (auto spaceId = 1; spaceId <= 2; spaceId++) {
        for (auto partId = 1; partId <= 6; partId++) {
            partMan->partsMap_[spaceId][partId] = PartHosts();
        }
    }

    fs::TempDir disk1("/tmp/nebula_store_test.XXXXXX");
    fs::TempDir disk2("/tmp/nebula_store_test.XXXXXX");

    KVOptions options;
    options.dataPaths_ = {disk1.path(), disk2.path()};
    options.partMan_ = std::move(partMan);
    HostAddr local = {"", 0};
    auto store = std::make_unique<NebulaStore>(std::move(options),
                                               ioThreadPool,
                                               local,
                                               getHandlers());
    store->init();
    sleep(1);
    EXPECT_EQ(2, store->spaces_.size());

    auto space1 = folly::stringPrintf("%s/nebula/%d", disk1.path(), 1);
    auto space2 = folly::stringPrintf("%s/nebula/%d", disk2.path(), 2);
    CHECK(boost::filesystem::exists(space1));
    CHECK(boost::filesystem::exists(space2));

    FLAGS_auto_remove_invalid_space = true;
    // remove space1, when the flag is true, the directory will be removed
    for (auto partId = 1; partId <= 6; partId++) {
        store->removePart(1, partId);
    }
    store->removeSpace(1, false);
    EXPECT_EQ(1, store->spaces_.size());
    CHECK(!boost::filesystem::exists(space1));
    CHECK(boost::filesystem::exists(space2));

    FLAGS_auto_remove_invalid_space = false;
    // remove space2, when the flag is false, the directory won't be removed
    for (auto partId = 1; partId <= 6; partId++) {
        store->removePart(2, partId);
    }
    store->removeSpace(2, false);
    EXPECT_EQ(0, store->spaces_.size());
    CHECK(!boost::filesystem::exists(space1));
    CHECK(boost::filesystem::exists(space2));
}

TEST(NebulaStoreTest, BackupRestoreTest) {
    GraphSpaceID spaceId = 1;
    PartitionID partId = 1;
    size_t vIdLen = kDefaultVidLen;

    fs::TempDir dataPath("/tmp/nebula_store_test_data_path.XXXXXX");
    fs::TempDir walPath("/tmp/nebula_store_test_wal_path.XXXXXX");
    fs::TempDir rocksdbWalPath("/tmp/nebula_store_test_rocksdb_wal_path.XXXXXX");
    fs::TempDir backupPath("/tmp/nebula_store_test_backup_path.XXXXXX");
    FLAGS_rocksdb_table_format = "PlainTable";
    FLAGS_rocksdb_wal_dir = rocksdbWalPath.path();
    FLAGS_rocksdb_backup_dir = backupPath.path();

    auto waitLeader = [] (const std::unique_ptr<NebulaStore>& store) {
        while (true) {
            int32_t leaderCount = 0;
            std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>> leaderIds;
            leaderCount += store->allLeader(leaderIds);
            if (leaderCount == 1) {
                break;
            }
            usleep(100000);
        }
    };

    auto test = [&] (bool insertData) {
        auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        std::vector<std::string> paths;
        paths.emplace_back(dataPath.path());
        auto partMan = std::make_unique<MemPartManager>();
        partMan->partsMap_[spaceId][partId] = PartHosts();

        KVOptions options;
        options.dataPaths_ = paths;
        options.walPath_ = walPath.path();
        options.partMan_ = std::move(partMan);
        HostAddr local = {"", 0};
        auto store = std::make_unique<NebulaStore>(std::move(options),
                                                   ioThreadPool,
                                                   local,
                                                   getHandlers());
        store->init();
        waitLeader(store);

        if (insertData) {
            std::vector<KV> data;
            for (auto tagId = 0; tagId < 10; tagId++) {
                data.emplace_back(NebulaKeyUtils::vertexKey(vIdLen, partId, "vertex", tagId),
                                  folly::stringPrintf("val_%d", tagId));
            }
            folly::Baton<true, std::atomic> baton;
            store->asyncMultiPut(spaceId, partId, std::move(data), [&] (cpp2::ErrorCode code) {
                EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                baton.post();
            });
            baton.wait();
        }

        {
            std::string prefix = NebulaKeyUtils::vertexPrefix(vIdLen, partId, "vertex");
            std::unique_ptr<KVIterator> iter;
            auto code = store->prefix(spaceId, partId, prefix, &iter);
            EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
            int32_t num = 0;
            while (iter->valid()) {
                num++;
                iter->next();
            }
            EXPECT_EQ(num, 10);
        }
    };

    // open rocksdb and write something
    test(true);
    // remove the data path to mock machine reboot
    CHECK(fs::FileUtils::remove(dataPath.path(), true));
    // recover from backup and check data
    test(false);

    FLAGS_rocksdb_table_format = "BlockBasedTable";
    FLAGS_rocksdb_wal_dir = "";
    FLAGS_rocksdb_backup_dir = "";
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
