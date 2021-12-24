/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "storage/cache/StorageCache.h"

DECLARE_uint32(storage_cache_capacity);

DECLARE_uint32(storage_cache_buckets_power);

DECLARE_uint32(storage_cache_locks_power);

DECLARE_uint32(vertex_pool_capacity);

namespace nebula {
namespace storage {
/*
 * summary:
 * 1. validate cache creation, adding pools, put and get values
 * 2. validate cache ttl
 * 3. validate error of adding pools with duplicate name
 * 4. validate error of putting into non-existing pool
 * */

class StorageCacheTest : public ::testing::Test {
 public:
  void SetUp() override {
    FLAGS_storage_cache_capacity = 1024;
    cache = std::make_unique<StorageCache>();
  }

  void TearDown() override {}

  std::string vertexKey1 = "test key";
  std::string vertexProp1 = "test prop";
  std::string vertexKeyNotExist = "key not exist";
  std::unique_ptr<StorageCache> cache;
};

TEST_F(StorageCacheTest, SimpleTest) {
  // creating cache instance
  bool ret = false;
  ret = cache->init();
  EXPECT_TRUE(ret);

  // adding vertext pool
  FLAGS_vertex_pool_capacity = 512;
  ret = cache->createVertexPool();
  EXPECT_TRUE(ret);
  EXPECT_EQ(cache->getVertexPoolSize(), FLAGS_vertex_pool_capacity);

  // put vertex
  ret = cache->putVertexProp(vertexKey1, vertexProp1, 60);
  EXPECT_TRUE(ret);

  // get value
  std::string value;
  ret = cache->getVertexProp(vertexKey1, &value);
  EXPECT_TRUE(ret);
  EXPECT_TRUE(value == vertexProp1);

  // get non-existing key
  ret = cache->getVertexProp(vertexKeyNotExist, &value);
  EXPECT_FALSE(ret);
}

TEST_F(StorageCacheTest, TestTTL) {
  // creating cache
  bool ret = false;
  ret = cache->init();
  EXPECT_TRUE(ret);

  // adding vertext pool
  FLAGS_vertex_pool_capacity = 512;
  ret = cache->createVertexPool();
  EXPECT_TRUE(ret);
  EXPECT_EQ(cache->getVertexPoolSize(), FLAGS_vertex_pool_capacity);

  // put vertex with ttl of 5
  ret = cache->putVertexProp(vertexKey1, vertexProp1, 2);
  EXPECT_TRUE(ret);

  // get value first
  std::string value;
  ret = cache->getVertexProp(vertexKey1, &value);
  EXPECT_TRUE(ret);
  EXPECT_TRUE(value == vertexProp1);

  // sleep 10s and get again
  std::this_thread::sleep_for(std::chrono::seconds(3));
  ret = cache->getVertexProp(vertexKey1, &value);
  EXPECT_FALSE(ret);
}

TEST_F(StorageCacheTest, DuplicatePoolName) {
  // creating cache
  bool ret = false;
  ret = cache->init();
  EXPECT_TRUE(ret);

  // adding vertext pool
  FLAGS_vertex_pool_capacity = 512;
  ret = cache->createVertexPool("pool");
  EXPECT_TRUE(ret);
  EXPECT_EQ(cache->getVertexPoolSize(), FLAGS_vertex_pool_capacity);

  // adding pool with the same name
  ret = cache->createVertexPool("pool");
  EXPECT_FALSE(ret);
}

TEST_F(StorageCacheTest, PutIntoNonExistingPool) {
  // creating cache
  bool ret = false;
  ret = cache->init();
  EXPECT_TRUE(ret);

  // put vertex with ttl of 5
  ret = cache->putVertexProp(vertexKey1, vertexProp1, 2);
  EXPECT_FALSE(ret);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
