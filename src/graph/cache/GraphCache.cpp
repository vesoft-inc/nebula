/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/cache/GraphCache.h"

DEFINE_uint32(graph_cache_capacity,
              100,
              "Total capacity reservered for graph in memory cache in MB");

DEFINE_uint32(graph_cache_buckets_power,
              10,
              "Number of buckets in base 2 logarithm. "
              "E.g., in case of 10, the total number of buckets will be 2^10.");

DEFINE_uint32(graph_cache_locks_power,
              5,
              "Number of locks in base 2 logarithm. "
              "E.g., in case of 5, the total number of buckets will be 2^5.");

DEFINE_uint32(vertex_pool_capacity, 50, "Vertex pool size in MB");

DEFINE_uint32(edge_pool_capacity, 50, "Edge pool size in MB");

DEFINE_uint32(vertex_item_ttl, 300, "Vertex item ttl in seconds");

DEFINE_uint32(edge_item_ttl, 300, "Edge item ttl in seconds");

namespace nebula {
namespace graph {

GraphCache::GraphCache() {
  capacity_ = FLAGS_graph_cache_capacity;
  cacheInternal_ = std::make_unique<CacheLibLRU>(
      kGraphCacheName, capacity_, FLAGS_graph_cache_buckets_power, FLAGS_graph_cache_locks_power);
}

bool GraphCache::init() {
  auto status = cacheInternal_->initializeCache();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }
  return true;
}

bool GraphCache::createVertexPool(std::string poolName) {
  auto status = cacheInternal_->addPool(poolName, FLAGS_vertex_pool_capacity);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }
  vertexPool_ = std::make_unique<VertexPoolInfo>(poolName, FLAGS_vertex_pool_capacity);
  return true;
}

bool GraphCache::createEdgePool(std::string poolName) {
  auto status = cacheInternal_->addPool(poolName, FLAGS_edge_pool_capacity);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }
  edgePool_ = std::make_unique<EdgePoolInfo>(poolName, FLAGS_edge_pool_capacity);
  return true;
}

StatusOr<std::vector<std::string>> GraphCache::getEdges(const std::string& key) {
  return cacheInternal_->getAsChain(key);
}

bool GraphCache::addAllEdges(const std::string& key, std::vector<std::string>& values) {
  if (!edgePool_) {
    LOG(ERROR) << "No edge pool exists!";
    return false;
  }
  auto status = cacheInternal_->putAsChain(key, values, edgePool_->poolName_, FLAGS_edge_item_ttl);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }
  return true;
}

void GraphCache::invalidateEdges(std::string& key) {
  cacheInternal_->invalidateItem(key);
}

uint32_t GraphCache::getEdgePoolSize() {
  auto ret = cacheInternal_->getConfiguredPoolSize(edgePool_->poolName_);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    return 0;
  }
  return ret.value() / 1024 / 1024;
}

bool GraphCache::getVertexProp(std::string& key, std::string* value) {
  auto ret = cacheInternal_->get(key);
  if (!ret.ok()) {
    return false;
  }
  value->assign(std::move(ret).value());
  return true;
}

bool GraphCache::putVertexProp(std::string& key, std::string& value) {
  if (!vertexPool_) {
    LOG(ERROR) << "No vertext pool exists!";
    return false;
  }
  auto status = cacheInternal_->put(key, value, vertexPool_->poolName_, FLAGS_vertex_item_ttl);
  if (!status.ok()) {
    return false;
  }
  return true;
}

void GraphCache::invalidateVertex(std::string& key) {
  cacheInternal_->invalidateItem(key);
}

uint32_t GraphCache::getVertexPoolSize() {
  auto ret = cacheInternal_->getConfiguredPoolSize(vertexPool_->poolName_);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    return 0;
  }
  return ret.value() / 1024 / 1024;
}

}  // namespace graph
}  // namespace nebula
