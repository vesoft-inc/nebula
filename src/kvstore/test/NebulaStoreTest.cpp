/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <iostream>
#include "fs/TempDir.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/RocksEngine.h"
#include "network/NetworkUtils.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

DECLARE_uint32(heartbeat_interval);

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
            partMan->partsMap_[spaceId][partId] = PartMeta();
        }
    }

    VLOG(1) << "Total space num is " << partMan->partsMap_.size()
            << ", total local partitions num is "
            << partMan->parts(HostAddr(0, 0)).size();

    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath.path()));
    paths.emplace_back(folly::stringPrintf("%s/disk2", rootPath.path()));

    KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    HostAddr local = {0, 0};
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

    store->asyncMultiPut(0, 0, {{"key", "val"}}, [](ResultCode code) {
        EXPECT_EQ(ResultCode::ERR_SPACE_NOT_FOUND, code);
    });

    store->asyncMultiPut(1, 6, {{"key", "val"}}, [](ResultCode code) {
        EXPECT_EQ(ResultCode::ERR_PART_NOT_FOUND, code);
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
    store->asyncMultiPut(1, 1, std::move(data), [&] (ResultCode code){
        EXPECT_EQ(ResultCode::SUCCEEDED, code);
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
    EXPECT_EQ(ResultCode::SUCCEEDED, store->range(1, 1, s, e, &iter));
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
    HostAddr local = {0, 0};
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
            PartMeta pm;
            pm.spaceId_ = 0;
            pm.partId_ = partId;
            pm.peers_ = peers;
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
    IPv4 ip;
    CHECK(network::NetworkUtils::ipv4ToInt("127.0.0.1", ip));
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
    int from = 0;
    while (true) {
        bool allLeaderElected = true;
        for (int part = from; part < 3; part++) {
            auto res = stores[0]->partLeader(0, part);
            CHECK(ok(res));
            auto leader = value(std::move(res));
            if (leader == HostAddr(0, 0)) {
                allLeaderElected = false;
                from = part;
                break;
            }
            LOG(INFO) << "Leader for part " << part << " is " << leader;
        }
        if (allLeaderElected) {
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
            stores[index]->asyncMultiPut(0, part, std::move(data), [&baton](ResultCode code){
                EXPECT_EQ(ResultCode::SUCCEEDED, code);
                baton.post();
            });
            baton.wait();
        }
        sleep(FLAGS_heartbeat_interval);
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
                ASSERT_EQ(ResultCode::SUCCEEDED, engine->range(s, e, &iter));
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
                                                 [&baton](ResultCode code) {
                EXPECT_EQ(ResultCode::ERR_LEADER_CHANGED, code);
                baton.post();
            });
            baton.wait();
        }
    }
}


}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
