/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/utils/OperationKeyUtils.h"
#include "kvstore/Part.h"
#include "kvstore/RocksEngine.h"

namespace nebula {
namespace kvstore {

const int32_t kDefaultVIdLen = 8;

void checkVertexData(RocksEngine* engine,
                     PartitionID partId,
                     int expectNum,
                     bool checkVal = false) {
  std::string vertexPrefix = NebulaKeyUtils::tagPrefix(partId);
  std::unique_ptr<KVIterator> iter;
  auto code = engine->prefix(vertexPrefix, &iter);
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  int32_t num = 0;
  while (iter->valid()) {
    num++;
    if (checkVal) {
      ASSERT_EQ(iter->val().str(), folly::stringPrintf("val%d", num));
    }
    iter->next();
  }
  ASSERT_EQ(num, expectNum);
}

void checkEdgeData(RocksEngine* engine, PartitionID partId, int expectNum) {
  std::string edgePrefix = NebulaKeyUtils::edgePrefix(partId);
  std::unique_ptr<KVIterator> iter;
  auto code = engine->prefix(edgePrefix, &iter);
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  int32_t num = 0;
  while (iter->valid()) {
    num++;
    iter->next();
  }
  ASSERT_EQ(num, expectNum);
}

void checkIndexData(RocksEngine* engine, PartitionID partId, int expectNum) {
  std::string indexPrefix = IndexKeyUtils::indexPrefix(partId);
  std::unique_ptr<KVIterator> iter;
  auto code = engine->prefix(indexPrefix, &iter);
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  int32_t num = 0;
  while (iter->valid()) {
    num++;
    iter->next();
  }
  ASSERT_EQ(num, expectNum);
}

TEST(PartTest, RocksTest) {
  fs::TempDir dataPath("/tmp/rocksdb_test.XXXXXX");
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
  CHECK(status.ok());
  LOG(INFO) << "Write data into rocksdb...";
  rocksdb::WriteBatch updates;
  std::vector<KV> kvs;
  for (uint32_t i = 0; i < 1000; i++) {
    kvs.emplace_back(folly::stringPrintf("key%d", i), folly::stringPrintf("val%d", i));
  }
  for (auto& kv : kvs) {
    updates.Put(rocksdb::Slice(kv.first), rocksdb::Slice(kv.second));
  }
  rocksdb::WriteOptions woptions;
  status = db->Write(woptions, &updates);
  CHECK(status.ok());
  LOG(INFO) << "Read data from rocksdb...";
  rocksdb::ReadOptions roptions;
  for (uint32_t i = 0; i < 1000; i++) {
    std::string val;
    status = db->Get(roptions, rocksdb::Slice(folly::stringPrintf("key%d", i)), &val);
    CHECK(status.ok());
    EXPECT_EQ(folly::stringPrintf("val%d", i), val);
  }

  LOG(INFO) << "Test finished...";
  delete db;
}

TEST(PartTest, KeyOrderTest) {
  fs::TempDir dataPath("/tmp/KeyOrderTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, dataPath.path());

  std::vector<KV> data;
  PartitionID partId = 1;

  // build vertex data in part 1, 2
  while (partId < 3) {
    auto key1 = NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, "", 0);
    data.emplace_back(key1, folly::stringPrintf("val%d", 1));

    auto key2 = NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, "", INT_MAX);
    data.emplace_back(key2, folly::stringPrintf("val%d", 2));

    auto key3 = NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, "ffffff", INT_MAX, '\377');
    data.emplace_back(key3, folly::stringPrintf("val%d", 3));

    auto key4 = NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, "", INT_MAX, '\377');
    data.emplace_back(key4, folly::stringPrintf("val%d", 4));

    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(data));
    data.clear();
    partId++;
  }
  {
    partId = 1;
    while (partId < 3) {
      checkVertexData(engine.get(), partId, 4, true);
      partId++;
    }
  }
}

TEST(PartTest, PartCleanTest) {
  fs::TempDir dataPath("/tmp/PartCleanTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, dataPath.path());

  std::vector<KV> data;
  PartitionID partId = 1;

  // build vertex data in part 1, 2
  while (partId < 3) {
    TagID tagId = 1;
    for (int i = 0; i < 10; i++) {
      auto key = NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, std::to_string(i), tagId);
      data.emplace_back(key, folly::stringPrintf("val%d", i));
    }
    tagId = 2;
    for (int i = 0; i < 10; i++) {
      auto key = NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, std::to_string(i), tagId);
      data.emplace_back(key, folly::stringPrintf("val%d", i));
    }

    EdgeType edgetype = 3;
    for (int i = 0; i < 10; i++) {
      auto key = NebulaKeyUtils::edgeKey(
          kDefaultVIdLen, partId, std::to_string(i), edgetype, 0, std::to_string(i));
      data.emplace_back(key, folly::stringPrintf("val%d", i));
    }
    IndexID indexId = 5;
    for (int i = 0; i < 10; i++) {
      auto keys = IndexKeyUtils::vertexIndexKeys(
          kDefaultVIdLen, partId, indexId, std::to_string(i), {"123"});
      for (auto& key : keys) {
        data.emplace_back(key, folly::stringPrintf("val%d", i));
      }
    }

    data.emplace_back(NebulaKeyUtils::systemCommitKey(partId), "123");
    data.emplace_back(NebulaKeyUtils::systemPartKey(partId), "");
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(data));
    data.clear();
    partId++;
  }

  {
    partId = 1;
    while (partId < 2) {
      checkVertexData(engine.get(), partId, 20);
      checkEdgeData(engine.get(), partId, 10);
      checkIndexData(engine.get(), partId, 10);
      {
        std::string val1;
        auto code1 = engine->get(NebulaKeyUtils::systemCommitKey(partId), &val1);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code1);
        ASSERT_EQ("123", val1);

        std::string val2;
        auto code2 = engine->get(NebulaKeyUtils::systemPartKey(partId), &val2);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code2);
      }

      partId++;
    }

    {
      // remove range part::clean data
      partId = 1;

      const auto& vertexPre = NebulaKeyUtils::tagPrefix(partId);
      auto ret = engine->removeRange(NebulaKeyUtils::firstKey(vertexPre, kDefaultVIdLen),
                                     NebulaKeyUtils::lastKey(vertexPre, kDefaultVIdLen));
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

      const auto& edgePre = NebulaKeyUtils::edgePrefix(partId);
      ret = engine->removeRange(NebulaKeyUtils::firstKey(edgePre, kDefaultVIdLen),
                                NebulaKeyUtils::lastKey(edgePre, kDefaultVIdLen));
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

      const auto& indexPre = IndexKeyUtils::indexPrefix(partId);
      ret = engine->removeRange(NebulaKeyUtils::firstKey(indexPre, sizeof(IndexID)),
                                NebulaKeyUtils::lastKey(indexPre, sizeof(IndexID)));
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

      const auto& operationPre = OperationKeyUtils::operationPrefix(partId);
      ret = engine->removeRange(NebulaKeyUtils::firstKey(operationPre, sizeof(int64_t)),
                                NebulaKeyUtils::lastKey(operationPre, sizeof(int64_t)));
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);

      ret = engine->remove(NebulaKeyUtils::systemCommitKey(partId));
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    }

    {
      // check data again
      partId = 1;
      checkVertexData(engine.get(), partId, 0);
      checkEdgeData(engine.get(), partId, 0);
      checkIndexData(engine.get(), partId, 0);
      std::string val1;
      auto code1 = engine->get(NebulaKeyUtils::systemCommitKey(partId), &val1);
      ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, code1);

      std::string val2;
      auto code2 = engine->get(NebulaKeyUtils::systemPartKey(partId), &val2);
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code2);
    }
    {
      partId = 2;
      checkVertexData(engine.get(), partId, 20);
      checkEdgeData(engine.get(), partId, 10);
      checkIndexData(engine.get(), partId, 10);
      std::string val1;
      auto code1 = engine->get(NebulaKeyUtils::systemCommitKey(partId), &val1);
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code1);

      std::string val2;
      auto code2 = engine->get(NebulaKeyUtils::systemPartKey(partId), &val2);
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code2);
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
