/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <iostream>
#include "fs/TempDir.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/RocksdbEngine.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace kvstore {

template<typename T>
void dump(const std::vector<T>& v) {
    std::stringstream ss;
    for (auto& e : v) {
        ss << e << ", ";
    }
    LOG(INFO) << ss.str();
}


TEST(KVStoreTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    auto partMan = std::make_unique<MemPartManager>();
    // GraphSpaceID =>  {PartitionIDs}
    // 1 => {0, 1, 2, 3, 4, 5}
    // 2 => {0, 1, 2, 3, 4, 5}
    for (auto spaceId = 1; spaceId <=2; spaceId++) {
        for (auto partId = 0; partId < 6; partId++) {
            partMan->partsMap_[spaceId][partId] = PartMeta();
        }
    }

    LOG(INFO) << "Total space num " << partMan->partsMap_.size()
              << ", " << partMan->parts(HostAddr(0, 0)).size();

    std::vector<std::string> paths;
    paths.push_back(folly::stringPrintf("%s/disk1", rootPath.path()));
    paths.push_back(folly::stringPrintf("%s/disk2", rootPath.path()));

    std::unique_ptr<NebulaStore> kv;
    KVOptions options;
    options.local_ = HostAddr(0, 0);
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    kv.reset(static_cast<NebulaStore*>(KVStore::instance(std::move(options))));
    EXPECT_EQ(2, kv->kvs_.size());

    EXPECT_EQ(6, kv->kvs_[1]->parts_.size());
    EXPECT_EQ(2, kv->kvs_[1]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1", rootPath.path()), kv->kvs_[1]->engines_[0].second);
    EXPECT_EQ(folly::stringPrintf("%s/disk2", rootPath.path()), kv->kvs_[1]->engines_[1].second);

    EXPECT_EQ(6, kv->kvs_[2]->parts_.size());
    EXPECT_EQ(2, kv->kvs_[2]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1", rootPath.path()), kv->kvs_[2]->engines_[0].second);
    EXPECT_EQ(folly::stringPrintf("%s/disk2", rootPath.path()), kv->kvs_[2]->engines_[1].second);

    kv->asyncMultiPut(0, 0, {{"key", "val"}}, [](ResultCode code, HostAddr addr) {
        UNUSED(addr);
        EXPECT_EQ(ResultCode::ERR_SPACE_NOT_FOUND, code);
    });

    kv->asyncMultiPut(1, 6, {{"key", "val"}}, [](ResultCode code, HostAddr addr) {
        UNUSED(addr);
        EXPECT_EQ(ResultCode::ERR_PART_NOT_FOUND, code);
    });

    LOG(INFO) << "Put some data then read them...";
    std::string prefix = "prefix";
    std::vector<KV> data;
    for (auto i = 0; i < 100; i++) {
        data.emplace_back(prefix + std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    kv->asyncMultiPut(1, 1, std::move(data), [](ResultCode code, HostAddr addr){
        UNUSED(addr);
        EXPECT_EQ(ResultCode::SUCCEEDED, code);
    });
    int32_t start = 0, end = 100;
    std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
    std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
    s = prefix + s;
    e = prefix + e;
    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(ResultCode::SUCCEEDED, kv->range(1, 1, s, e, &iter));
    int num = 0;
    while (iter->valid()) {
        auto key = *reinterpret_cast<const int32_t*>(iter->key().data() + prefix.size());
        auto val = iter->val();
        EXPECT_EQ(num, key);
        EXPECT_EQ(folly::stringPrintf("val_%d", num), val);
        iter->next();
        num++;
    }
    EXPECT_EQ(100, num);
}


TEST(KVStoreTest, PartsTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    auto partMan = std::make_unique<MemPartManager>();
    // GraphSpaceID =>  {PartitionIDs}
    // 0 => {0, 1, 2, 3...9}
    // The parts on PartMan is 0...9
    for (auto partId = 0; partId < 10; partId++) {
        partMan->addPart(0, partId);
    }
    std::vector<std::string> paths;
    paths.push_back(folly::stringPrintf("%s/disk1", rootPath.path()));
    paths.push_back(folly::stringPrintf("%s/disk2", rootPath.path()));
    for (size_t i = 0; i < paths.size(); i++) {
        auto db = std::make_unique<RocksdbEngine>(0,
                            folly::stringPrintf("%s/nebula/%d/data", paths[i].c_str(), 0));
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

    std::unique_ptr<NebulaStore> kv;
    KVOptions options;
    options.local_ = HostAddr(0, 0);
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    kv.reset(static_cast<NebulaStore*>(KVStore::instance(std::move(options))));

    auto check = [&](GraphSpaceID spaceId) {
        // After init, the parts should be 0-9, and the distribution should be
        // disk1: 0, 1, 2, x, y
        // disk2: 5, 6, 7, x1, y1
        // x, y, x1, y1 in {3, 4, 8, 9}
        for (auto i = 0; i < 2; i++) {
            ASSERT_EQ(folly::stringPrintf("%s/disk%d", rootPath.path(), i + 1),
                      kv->kvs_[spaceId]->engines_[i].second);
            auto parts = kv->kvs_[spaceId]->engines_[i].first->allParts();
            dump(parts);
            ASSERT_EQ(5, parts.size());
        }
    };
    check(0);
    auto* pm = static_cast<MemPartManager*>(kv->partMan_.get());
    // Let's create another space with 10 parts.
    for (auto partId = 0; partId < 10; partId++) {
        pm->addPart(1, partId);
    }
    check(1);
    // Let's remove space some parts in GraphSpace 0
    for (auto partId = 0; partId < 5; partId++) {
        pm->removePart(0, partId);
    }
    int32_t totalParts = 0;
    for (auto i = 0; i < 2; i++) {
        ASSERT_EQ(folly::stringPrintf("%s/disk%d", rootPath.path(), i + 1),
                  kv->kvs_[0]->engines_[i].second);
        auto parts = kv->kvs_[0]->engines_[i].first->allParts();
        dump(parts);
        totalParts += parts.size();
    }
    ASSERT_EQ(5, totalParts);
    for (auto partId = 5; partId < 10; partId++) {
        pm->removePart(0, partId);
    }
    ASSERT_TRUE(kv->kvs_.find(0) == kv->kvs_.end());
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


