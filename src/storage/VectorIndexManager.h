/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_VECTORINDEXMANAGER_H_
#define STORAGE_VECTORINDEXMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/futures/Future.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "common/thrift/ThriftTypes.h"
#include "common/vectorIndex/VectorIndex.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace storage {

// Key type for identifying vector indexes: <PartitionID, IndexID>
using VectorIndexKey = std::pair<PartitionID, IndexID>;

// Custom hash function for VectorIndexKey
struct VectorIndexKeyHash {
  std::size_t operator()(const VectorIndexKey& key) const {
    return std::hash<PartitionID>()(key.first) ^ (std::hash<IndexID>()(key.second) << 1);
  }
};

/**
 * @brief VectorIndexManager is a singleton component that runs independently
 * in storaged process to manage vector indexes. It maintains a map of
 * <PartitionID, IndexID> -> AnnIndex smart pointers.
 */
class VectorIndexManager final {
 public:
  static VectorIndexManager& getInstance();

  // Initialize the manager with necessary dependencies
  Status init(kvstore::KVStore* kvstore,
              meta::SchemaManager* schemaManager,
              meta::IndexManager* indexManager);

  // Start the manager (background tasks, cleanup threads, etc.)
  Status start();

  // Stop the manager gracefully
  Status stop();

  // Create a vector index for a specific partition and index ID
  Status createOrUpdateIndex(GraphSpaceID spaceId,
                             PartitionID partitionId,
                             IndexID indexId,
                             const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem);

  // Get an existing vector index
  StatusOr<std::shared_ptr<AnnIndex>> getIndex(GraphSpaceID spaceId,
                                               PartitionID partitionId,
                                               IndexID indexId);

  // Remove a vector index
  Status removeIndex(GraphSpaceID spaceId, PartitionID partitionId, IndexID indexId);

  // Add vectors to an index
  Status addVectors(GraphSpaceID spaceId,
                    PartitionID partitionId,
                    IndexID indexId,
                    const VecData& vecData);

  // Search vectors in an index
  StatusOr<SearchResult> searchVectors(GraphSpaceID spaceId,
                                       PartitionID partitionId,
                                       IndexID indexId,
                                       const SearchParams& searchParams);

  // Get all managed indexes for a partition
  std::vector<std::shared_ptr<AnnIndex>> getIndexesByPartition(GraphSpaceID spaceId,
                                                               PartitionID partitionId);

  // Check if an index exists
  bool hasIndex(GraphSpaceID spaceId, PartitionID partitionId, IndexID indexId) const;

  // Rebuild an index (called during BuildVectorIndexTask)
  Status rebuildIndex(GraphSpaceID spaceId,
                      PartitionID partitionId,
                      IndexID indexId,
                      const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem);

  size_t getIndexId() const {
    auto ret = indexId_.load();
    indexId_.fetch_add(1);
    return ret;
  }

  // Non-copyable and non-movable
  VectorIndexManager(const VectorIndexManager&) = delete;
  VectorIndexManager& operator=(const VectorIndexManager&) = delete;
  VectorIndexManager(VectorIndexManager&&) = delete;
  VectorIndexManager& operator=(VectorIndexManager&&) = delete;

 private:
  VectorIndexManager() = default;
  ~VectorIndexManager() = default;

  // Helper methods
  std::shared_ptr<AnnIndex> createIndex(GraphSpaceID spaceId,
                                        PartitionID partitionId,
                                        IndexID indexId,
                                        const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem);

  Status validateIndexItem(const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem);

  std::string getIndexRootPath(GraphSpaceID spaceId, PartitionID partitionId, IndexID indexId);

  // Background cleanup task
  void cleanupExpiredIndexes();
  void backgroundCleanup();

  // Thread-safe index map
  mutable std::shared_mutex indexMapMutex_;
  std::unordered_map<
      GraphSpaceID,
      std::unordered_map<VectorIndexKey, std::shared_ptr<AnnIndex>, VectorIndexKeyHash>>
      indexMap_;

  // Dependencies
  kvstore::KVStore* kvstore_{nullptr};
  meta::SchemaManager* schemaManager_{nullptr};
  meta::IndexManager* indexManager_{nullptr};

  // Manager state
  std::atomic<bool> running_{false};
  std::atomic<bool> stopped_{false};

  // Background cleanup thread
  std::unique_ptr<std::thread> cleanupThread_;
  std::mutex cleanupMutex_;
  std::condition_variable cleanupCondVar_;
  mutable std::atomic<size_t> indexId_{0};

  // Configuration
  static constexpr size_t kCleanupIntervalSeconds = 300;  // 5 minutes
  static constexpr size_t kMaxIndexesPerPartition = 100;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_VECTORINDEXMANAGER_H_
