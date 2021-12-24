/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/cache/GraphCache.h"

DECLARE_uint32(graph_cache_capacity);

DECLARE_uint32(graph_cache_buckets_power);

DECLARE_uint32(graph_cache_locks_power);

DECLARE_uint32(edge_pool_capacity);

DECLARE_uint32(edge_item_ttl);

namespace nebula {
namespace graph {

/**
 * summary:
 * 1. validate cache creation, adding pools, put and get values
 * 2. validate cache ttl
 */
class GraphCacheTest : public ::testing::Test {
 public:
  void SetUp() override {
    FLAGS_graph_cache_capacity = 1024;
    // cache = std::make_unique<GraphCache>();
    cache = GraphCache::instance();
  }

  void TearDown() override {}

  std::string graphKey1 = "test key";
  std::vector<std::string> edges1 = {"edge1", "edge2", "edge3"};
  std::string vertexKeyNotExist = "key not exist";
  // std::unique_ptr<GraphCache> cache;
  GraphCache* cache{nullptr};
};

TEST_F(GraphCacheTest, SimpleTest) {
  // creating cache instance
  bool ret = false;
  ret = cache->init();
  EXPECT_TRUE(ret);

  // adding vertext pool
  FLAGS_edge_pool_capacity = 512;
  ret = cache->createEdgePool();
  EXPECT_TRUE(ret);
  EXPECT_EQ(cache->getEdgePoolSize(), FLAGS_edge_pool_capacity);

  // put edges
  FLAGS_edge_item_ttl = 60;
  ret = cache->addAllEdges(graphKey1, edges1);
  EXPECT_TRUE(ret);

  // get edges
  auto status = cache->getEdges(graphKey1);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.value().size(), 3);
}

TEST_F(GraphCacheTest, TestTTL) {
  // creating cache instance
  bool ret = false;
  ret = cache->init();
  EXPECT_TRUE(ret);

  // adding vertext pool
  FLAGS_edge_pool_capacity = 512;
  ret = cache->createEdgePool();
  EXPECT_TRUE(ret);
  EXPECT_EQ(cache->getEdgePoolSize(), FLAGS_edge_pool_capacity);

  // put vertex with ttl of 2
  FLAGS_edge_item_ttl = 2;
  ret = cache->addAllEdges(graphKey1, edges1);
  EXPECT_TRUE(ret);

  // get edges
  std::vector<std::string> edges;
  auto status = cache->getEdges(graphKey1);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.value().size(), 3);

  // sleep 3s and get again
  std::this_thread::sleep_for(std::chrono::seconds(3));
  status = cache->getEdges(graphKey1);
  EXPECT_FALSE(status.ok());
}

}  // namespace graph
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
