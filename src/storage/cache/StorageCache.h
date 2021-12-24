/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_CACHE_STORAGECACHE_H
#define STORAGE_CACHE_STORAGECACHE_H

#include "common/base/Base.h"
#include "common/base/CacheLibLRU.h"

namespace nebula {
namespace storage {

static const char kVertexPoolName[] = "VertextPool";
static const char kStorageCacheName[] = "__StorageCache__";

struct VertexPoolInfo {
  std::string poolName_;
  uint32_t capacity_;
  VertexPoolInfo(std::string poolName, uint32_t capacity)
      : poolName_(poolName), capacity_(capacity) {}
};

class StorageCache {
 public:
  StorageCache();

  ~StorageCache() {
    LOG(INFO) << "Destroy storage cache";
  }

  bool init();

  // create a vertex cache as pool
  bool createVertexPool(std::string poolName = kVertexPoolName);

  // create a edge cache as pool
  bool createEdgePool(std::string& key, std::string* value);

  // get vertex property via key
  bool getVertexProp(std::string& key, std::string* value);

  // insert or update vertex property in cache
  bool putVertexProp(std::string key, std::string value, uint32_t ttl);

  // evict a vertex in cache
  void invalidateVertex(std::string& key);

  // get the size of the vertex pool
  uint32_t getVertexPoolSize();

 private:
  uint32_t capacity_ = 0;  // in MB
  std::unique_ptr<CacheLibLRU> cacheInternal_{nullptr};
  std::shared_ptr<VertexPoolInfo> vertexPool_{nullptr};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_CACHE_STORAGECACHE_H
