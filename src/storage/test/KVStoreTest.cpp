/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/include/KVStore.h"
#include "storage/PartManager.h"
#include "storage/KVStoreImpl.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace storage {

TEST(KVStoreTest, SimpleTest) {
    FLAGS_part_man_type = "memory";  // Use MemPartManager.
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    MemPartManager* partMan = reinterpret_cast<MemPartManager*>(PartManager::instance());
    // GraphSpaceID =>  {PartitionIDs}
    // 1 => {0, 1, 2, 3, 4, 5}
    // 2 => {0, 1, 2, 3, 4, 5}
    for (auto spaceId = 1; spaceId <=2; spaceId++) {
        for (auto partId = 0; partId < 6; partId++) {
            partMan->partsMap_[spaceId][partId] = PartMeta();
        }
    }

    LOG(INFO) << "Total space num " << partMan->partsMap_.size() << ", " << partMan->parts(HostAddr(0, 0)).size();

    auto dataPath = folly::stringPrintf("%s/disk1, %s/disk2", rootPath.path(), rootPath.path());

    std::vector<std::string> paths;
    paths.push_back(folly::stringPrintf("%s/disk1", rootPath.path()));
    paths.push_back(folly::stringPrintf("%s/disk2", rootPath.path()));

    KVStoreImpl* kv = reinterpret_cast<KVStoreImpl*>(KVStore::instance(HostAddr(0, 0),
                                                                       std::move(paths)));
    EXPECT_EQ(2, kv->kvs_.size());

    EXPECT_EQ(6, kv->kvs_[1]->parts_.size());
    EXPECT_EQ(2, kv->kvs_[1]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1", rootPath.path()), kv->kvs_[1]->engines_[0].second);
    EXPECT_EQ(folly::stringPrintf("%s/disk2", rootPath.path()), kv->kvs_[1]->engines_[1].second);

    EXPECT_EQ(6, kv->kvs_[2]->parts_.size());
    EXPECT_EQ(2, kv->kvs_[2]->engines_.size());
    EXPECT_EQ(folly::stringPrintf("%s/disk1", rootPath.path()), kv->kvs_[2]->engines_[0].second);
    EXPECT_EQ(folly::stringPrintf("%s/disk2", rootPath.path()), kv->kvs_[2]->engines_[1].second);

    auto shouldNotReach =  [](ResultCode code, HostAddr addr){
        UNUSED(code);
        UNUSED(addr);
        LOG(FATAL) << "Should not reach here";
    };

    EXPECT_EQ(ResultCode::ERR_SPACE_NOT_FOUND,
              kv->asyncMultiPut(0, 0, {{"key", "val"}}, shouldNotReach));

    EXPECT_EQ(ResultCode::ERR_PART_NOT_FOUND,
              kv->asyncMultiPut(1, 6, {{"key", "val"}}, shouldNotReach));

    LOG(INFO) << "Put some data then read them...";
    std::vector<KV> data;
    for (auto i = 0; i < 100; i++) {
        data.emplace_back(std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(ResultCode::SUCCESSED,
              kv->asyncMultiPut(1, 1, std::move(data), [](ResultCode code, HostAddr addr){
                  UNUSED(addr);
                  EXPECT_EQ(ResultCode::SUCCESSED, code);
              }));
    int32_t start = 0, end = 100;
    std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
    std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
    std::unique_ptr<StorageIter> iter;
    EXPECT_EQ(ResultCode::SUCCESSED, kv->range(1, 1, s, e, iter));
    int num = 0;
    while (iter->valid()) {
        auto key = *reinterpret_cast<const int32_t*>(iter->key().data());
        auto val = iter->val();
        EXPECT_EQ(num, key);
        EXPECT_EQ(folly::stringPrintf("val_%d", num), val);
        iter->next();
        num++;
    }
    EXPECT_EQ(100, num);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


