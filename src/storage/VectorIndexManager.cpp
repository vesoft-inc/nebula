/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/VectorIndexManager.h"

#include <fmt/format.h>
#include <folly/hash/Hash.h>

#include "common/base/Logging.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "common/utils/Utils.h"
#include "common/vectorIndex/HNSWIndex.h"
#include "common/vectorIndex/IVFIndex.h"
#include "common/vectorIndex/VectorIndexUtils.h"

namespace nebula {
namespace storage {

VectorIndexManager& VectorIndexManager::getInstance() {
  static VectorIndexManager instance;
  return instance;
}

Status VectorIndexManager::init(kvstore::KVStore* kvstore,
                                meta::SchemaManager* schemaManager,
                                meta::IndexManager* indexManager) {
  if (kvstore == nullptr || schemaManager == nullptr || indexManager == nullptr) {
    return Status::Error("Invalid parameters for VectorIndexManager initialization");
  }

  kvstore_ = kvstore;
  schemaManager_ = schemaManager;
  indexManager_ = indexManager;

  LOG(INFO) << "VectorIndexManager initialized successfully";
  return Status::OK();
}

Status VectorIndexManager::start() {
  if (running_.load()) {
    return Status::Error("VectorIndexManager is already running");
  }

  running_.store(true);
  stopped_.store(false);

  // Start background cleanup thread
  cleanupThread_ = std::make_unique<std::thread>([this]() { this->backgroundCleanup(); });

  LOG(INFO) << "VectorIndexManager started successfully";
  return Status::OK();
}

Status VectorIndexManager::stop() {
  if (stopped_.load()) {
    return Status::OK();
  }

  running_.store(false);

  // Notify cleanup thread to stop
  {
    std::lock_guard<std::mutex> lock(cleanupMutex_);
    cleanupCondVar_.notify_all();
  }

  // Wait for cleanup thread to finish
  if (cleanupThread_ && cleanupThread_->joinable()) {
    cleanupThread_->join();
  }
  cleanupThread_.reset();

  // Clear all indexes
  {
    std::unique_lock<std::shared_mutex> lock(indexMapMutex_);
    indexMap_.clear();
  }

  stopped_.store(true);
  LOG(INFO) << "VectorIndexManager stopped successfully";
  return Status::OK();
}

Status VectorIndexManager::createOrUpdateIndex(
    GraphSpaceID spaceId,
    PartitionID partitionId,
    IndexID indexId,
    const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem) {
  // Validate input parameters
  auto validateStatus = validateIndexItem(indexItem);
  if (!validateStatus.ok()) {
    return validateStatus;
  }

  VectorIndexKey key{partitionId, indexId};

  // Check if index already exists
  {
    std::shared_lock<std::shared_mutex> readLock(indexMapMutex_);
    if (!indexMap_.empty()) {
      auto it = indexMap_.find(spaceId);
      if (it != indexMap_.end()) {
        LOG(INFO) << "Vector index already exists for partition " << partitionId << ", index "
                  << indexId << ", updating it";
      }
      auto index = it->second.find(key);
      if (index != it->second.end()) {
        LOG(INFO) << "Vector index already exists for partition " << partitionId << ", index "
                  << indexId << ", updating it";
      }
    }
  }
  LOG(ERROR) << "Vector index not found for partition " << partitionId << ", index " << indexId;
  // Create new index
  auto newIndex = createIndex(spaceId, partitionId, indexId, indexItem);
  if (!newIndex) {
    return Status::Error("Failed to create vector index");
  }
  // Add to index map
  {
    std::unique_lock<std::shared_mutex> writeLock(indexMapMutex_);
    indexMap_[spaceId][key] = newIndex;
  }

  LOG(INFO) << "Successfully created/updated vector index for space " << spaceId << ", partition "
            << partitionId << ", index " << indexId;
  return Status::OK();
}

StatusOr<std::shared_ptr<AnnIndex>> VectorIndexManager::getIndex(GraphSpaceID spaceId,
                                                                 PartitionID partitionId,
                                                                 IndexID indexId) {
  VectorIndexKey key{partitionId, indexId};

  std::shared_lock<std::shared_mutex> readLock(indexMapMutex_);
  if (indexMap_.empty()) {
    LOG(ERROR) << "No vector indexes found";
    return Status::Error("No vector indexes found");
  }
  auto it = indexMap_.find(spaceId);
  if (it == indexMap_.end()) {
    LOG(ERROR) << "Vector index not found for partition " << partitionId << ", index " << indexId;
    return Status::Error("Vector index not found for partition %d, index %d", partitionId, indexId);
  }
  auto index = it->second.find(key);
  if (index == it->second.end()) {
    LOG(ERROR) << "Vector index not found for partition " << partitionId << ", index " << indexId;
    return Status::Error("Vector index not found for partition %d, index %d", partitionId, indexId);
  }

  return index->second;
}

Status VectorIndexManager::removeIndex(GraphSpaceID spaceId,
                                       PartitionID partitionId,
                                       IndexID indexId) {
  VectorIndexKey key{partitionId, indexId};

  std::unique_lock<std::shared_mutex> writeLock(indexMapMutex_);
  auto it = indexMap_.find(spaceId);
  if (it == indexMap_.end()) {
    return Status::Error("Vector index not found for partition %d, index %d", partitionId, indexId);
  }
  auto index = it->second.find(key);
  if (index == it->second.end()) {
    return Status::Error("Vector index not found for partition %d, index %d", partitionId, indexId);
  }

  indexMap_.erase(it);
  LOG(INFO) << "Successfully removed vector index for partition " << partitionId << ", index "
            << indexId;
  return Status::OK();
}

Status VectorIndexManager::addVectors(GraphSpaceID spaceId,
                                      PartitionID partitionId,
                                      IndexID indexId,
                                      const VecData& vecData) {
  auto indexOrError = getIndex(spaceId, partitionId, indexId);
  if (!indexOrError.ok()) {
    return indexOrError.status();
  }

  auto index = indexOrError.value();
  LOG(ERROR) << "Begin to add vectors";
  return index->add(&vecData);
}

StatusOr<SearchResult> VectorIndexManager::searchVectors(GraphSpaceID spaceId,
                                                         PartitionID partitionId,
                                                         IndexID indexId,
                                                         const SearchParams& searchParams) {
  auto indexOrError = getIndex(spaceId, partitionId, indexId);
  if (!indexOrError.ok()) {
    return indexOrError.status();
  }

  auto index = indexOrError.value();
  SearchResult result;
  auto status = index->search(&searchParams, &result);
  if (!status.ok()) {
    return status;
  }

  return result;
}

bool VectorIndexManager::hasIndex(GraphSpaceID spaceId,
                                  PartitionID partitionId,
                                  IndexID indexId) const {
  VectorIndexKey key{partitionId, indexId};

  std::shared_lock<std::shared_mutex> readLock(indexMapMutex_);
  auto it = indexMap_.find(spaceId);
  if (it != indexMap_.end()) {
    return it->second.find(key) != it->second.end();
  }
  return false;
}

Status VectorIndexManager::rebuildIndex(
    GraphSpaceID spaceId,
    PartitionID partitionId,
    IndexID indexId,
    const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem) {
  // Remove existing index if it exists
  removeIndex(spaceId, partitionId, indexId);

  // Create new index
  return createOrUpdateIndex(spaceId, partitionId, indexId, indexItem);
}

std::shared_ptr<AnnIndex> VectorIndexManager::createIndex(
    GraphSpaceID spaceId,
    PartitionID partitionId,
    IndexID indexId,
    const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem) {
  // Extract parameters from indexItem
  std::string indexName = indexItem->get_index_name();
  // Get ANN parameters
  const auto& annParams = *indexItem->get_ann_params();
  if (annParams.empty()) {
    LOG(ERROR) << "Empty ANN parameters for index " << indexId;
    return nullptr;
  }

  std::string indexType = annParams[0];
  size_t dim =
      folly::to<size_t>(annParams[1]);  // Default dimension, should be extracted from schema
  MetricType metricType = MetricType::L2;
  std::string lowerMetric = annParams[2];
  std::transform(lowerMetric.begin(), lowerMetric.end(), lowerMetric.begin(), ::tolower);
  if (lowerMetric.find("inner") != std::string::npos) {
    metricType = MetricType::INNER_PRODUCT;
  }

  // Get root path for index files
  std::string rootPath = getIndexRootPath(spaceId, partitionId, indexId);

  std::shared_ptr<AnnIndex> index;

  if (indexType == "IVF") {
    // Create IVF index
    size_t nlist = annParams.size() > 3 ? folly::to<size_t>(annParams[3]) : 8;
    size_t trainSize = annParams.size() > 4 ? folly::to<size_t>(annParams[4]) : 10;
    LOG(ERROR) << "IVF params: "
               << "nlist: " << nlist << ", trainsize: " << trainSize;
    index = std::make_shared<IVFIndex>(spaceId,
                                       partitionId,
                                       indexId,
                                       indexName,
                                       true,  // propFromNode
                                       dim,
                                       rootPath,
                                       metricType,
                                       trainSize);

    // Initialize with build parameters
    auto buildParams =
        std::make_unique<BuildParamsIVF>(metricType, AnnIndexType::IVF, nlist, trainSize);
    auto status = index->init(buildParams.get());
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize IVF index: " << status.toString();
      return nullptr;
    }
    LOG(ERROR) << "Build IVF Index";

  } else if (indexType == "HNSW") {
    // Create HNSW index
    size_t M = annParams.size() > 3 ? folly::to<size_t>(annParams[3]) : 16;
    size_t efConstruction = annParams.size() > 4 ? folly::to<size_t>(annParams[4]) : 200;
    size_t capacity = annParams.size() > 5 ? folly::to<size_t>(annParams[5]) : 10000;
    LOG(ERROR) << "HNSW params: "
               << "M: " << M << ", efConstruction: " << efConstruction
               << ", capacity: " << capacity;
    index = std::make_shared<HNSWIndex>(spaceId,
                                        partitionId,
                                        indexId,
                                        indexName,
                                        true,  // propFromNode
                                        dim,
                                        rootPath,
                                        metricType);

    // Initialize with build parameters
    auto buildParams = std::make_unique<BuildParamsHNSW>(metricType, M, efConstruction, capacity);
    auto status = index->init(buildParams.get());
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize HNSW index: " << status.toString();
      return nullptr;
    }
    LOG(ERROR) << "Build HNSW Index";
  } else {
    LOG(ERROR) << "Unsupported index type: " << indexType;
    return nullptr;
  }

  LOG(INFO) << "Successfully created " << indexType << " index for space " << spaceId
            << ", partition " << partitionId << ", index " << indexId;
  return index;
}

Status VectorIndexManager::validateIndexItem(
    const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem) {
  if (indexItem->get_index_name().empty()) {
    return Status::Error("Index name cannot be empty");
  }

  if (indexItem->get_prop_name().empty()) {
    return Status::Error("Property name cannot be empty");
  }

  const auto& annParams = *indexItem->get_ann_params();
  if (annParams.empty()) {
    return Status::Error("ANN parameters cannot be empty");
  }

  std::string indexType = annParams[0];
  if (indexType != "IVF" && indexType != "HNSW") {
    return Status::Error("Unsupported index type: %s", indexType.c_str());
  }

  return Status::OK();
}

std::string VectorIndexManager::getIndexRootPath(GraphSpaceID spaceId,
                                                 PartitionID partitionId,
                                                 IndexID indexId) {
  // Create a hierarchical path: /vector_indexes/{spaceId}/{partitionId}/{indexId}/
  return fmt::format("/vector_indexes/{}/{}/{}/", spaceId, partitionId, indexId);
}

void VectorIndexManager::cleanupExpiredIndexes() {
  // Implementation for cleaning up expired or unused indexes
  // This could include removing indexes for dropped spaces, partitions, etc.
  LOG(INFO) << "Running vector index cleanup";

  // Get current stats before cleanup
  LOG(INFO) << "Performing vector index cleanup";

  // TODO: Implement actual cleanup logic
  // This would involve checking for:
  // - Dropped spaces
  // - Dropped partitions
  // - Removed indexes
  // And cleaning up corresponding vector index data
}

void VectorIndexManager::backgroundCleanup() {
  LOG(INFO) << "Vector index background cleanup thread started";

  while (running_.load()) {
    try {
      std::unique_lock<std::mutex> lock(cleanupMutex_);
      if (cleanupCondVar_.wait_for(lock, std::chrono::seconds(kCleanupIntervalSeconds), [this]() {
            return !running_.load();
          })) {
        // Condition variable was notified (probably shutdown)
        break;
      }
      // Perform cleanup
      cleanupExpiredIndexes();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Exception in vector index cleanup thread: " << e.what();
    }
  }

  LOG(INFO) << "Vector index background cleanup thread stopped";
}

}  // namespace storage
}  // namespace nebula
