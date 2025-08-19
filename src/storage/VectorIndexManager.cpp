/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/VectorIndexManager.h"

#include <fmt/format.h>
#include <folly/hash/Hash.h>

#include "common/base/Logging.h"
#include "common/fs/FileUtils.h"
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

Status VectorIndexManager::init(meta::IndexManager* indexManager, std::string annIndexPath) {
  if (indexManager == nullptr || annIndexPath.empty()) {
    return Status::Error("Invalid parameters for VectorIndexManager initialization");
  }

  indexManager_ = indexManager;
  annIndexPath_ = std::move(annIndexPath);
  if (!fs::FileUtils::exist(annIndexPath_)) {
    auto status = fs::FileUtils::makeDir(annIndexPath_);
    if (!status) {
      LOG(ERROR) << "Make ann index path failed";
    }
  }
  // Load existing indexes from disk if any
  auto loadStatus = loadExistingIndexes();
  if (!loadStatus.ok()) {
    LOG(WARNING) << "Failed to load existing indexes: " << loadStatus.toString();
  }

  LOG(INFO) << "VectorIndexManager initialized successfully";
  return Status::OK();
}

Status VectorIndexManager::start() {
  if (running_.load()) {
    return Status::Error("VectorIndexManager is already running");
  }

  running_.store(true);
  stopped_.store(false);

  LOG(INFO) << "VectorIndexManager started successfully";
  return Status::OK();
}

Status VectorIndexManager::stop() {
  writeAnnIndexesToDisk();
  running_.store(false);
  stopped_.store(true);
  // Clear all indexes
  {
    std::unique_lock<std::shared_mutex> lock(indexMapMutex_);
    indexMap_.clear();
  }
  LOG(INFO) << "VectorIndexManager stopped successfully";
  return Status::OK();
}

void VectorIndexManager::waitUntilStop() {
  std::unique_lock<std::mutex> lkStop(muStop_);
  while (!stopped_.load()) {
    cvStop_.wait(lkStop);
  }
  this->stop();
}

void VectorIndexManager::notifyStop() {
  std::unique_lock<std::mutex> lkStop(muStop_);
  if (!stopped_.load()) {
    running_.store(false);
    stopped_.store(true);
    cvStop_.notify_all();
  }
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
  return index->add(&vecData, false);
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
  removeIndex(spaceId, partitionId, indexId);
  return createOrUpdateIndex(spaceId, partitionId, indexId, indexItem);
}

std::shared_ptr<AnnIndex> VectorIndexManager::createIndex(
    GraphSpaceID spaceId,
    PartitionID partitionId,
    IndexID indexId,
    const std::shared_ptr<meta::cpp2::AnnIndexItem>& indexItem) {
  std::string indexName = indexItem->get_index_name();
  const auto& annParams = *indexItem->get_ann_params();
  if (annParams.empty()) {
    LOG(ERROR) << "Empty ANN parameters for index " << indexId;
    return nullptr;
  }
  std::string indexType = annParams[0];
  size_t dim = folly::to<size_t>(annParams[1]);
  MetricType metricType = MetricType::L2;
  std::string lowerMetric = annParams[2];
  std::transform(lowerMetric.begin(), lowerMetric.end(), lowerMetric.begin(), ::tolower);
  if (lowerMetric.find("inner") != std::string::npos) {
    metricType = MetricType::INNER_PRODUCT;
  }
  std::string rootPath = getAnnIndexPath(spaceId, partitionId, indexId);
  std::shared_ptr<AnnIndex> index;
  if (indexType == "IVF") {
    // Create IVF index
    size_t nlist = annParams.size() > 3 ? folly::to<size_t>(annParams[3]) : 8;
    size_t trainSize = annParams.size() > 4 ? folly::to<size_t>(annParams[4]) : 10;
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
  } else if (indexType == "HNSW") {
    // Create HNSW index
    size_t M = annParams.size() > 3 ? folly::to<size_t>(annParams[3]) : 16;
    size_t efConstruction = annParams.size() > 4 ? folly::to<size_t>(annParams[4]) : 200;
    size_t capacity = annParams.size() > 5 ? folly::to<size_t>(annParams[5]) : 10000;
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

size_t VectorIndexManager::getIndexSize(GraphSpaceID spaceId,
                                        PartitionID partitionId,
                                        IndexID indexId) const {
  VectorIndexKey key{partitionId, indexId};

  std::shared_lock<std::shared_mutex> readLock(indexMapMutex_);
  auto it = indexMap_.find(spaceId);
  if (it != indexMap_.end()) {
    auto index = it->second.find(key);
    if (index != it->second.end()) {
      return index->second->size();
    }
  }
  return 0;
}

std::string VectorIndexManager::getAnnIndexPath(GraphSpaceID spaceId,
                                                PartitionID partitionId,
                                                IndexID indexId) {
  return fmt::format("{}/{}_{}_{}", annIndexPath_, spaceId, partitionId, indexId);
}

Status VectorIndexManager::loadExistingIndexes() {
  if (annIndexPath_.empty()) {
    LOG(WARNING) << "ANN index path is empty, skipping load existing indexes";
    return Status::OK();
  }

  try {
    auto fileType = fs::FileUtils::fileType(annIndexPath_.c_str());
    if (fileType != fs::FileType::DIRECTORY) {
      LOG(ERROR) << "ANN index directory does not exist: " << annIndexPath_;
      return Status::OK();
    }

    std::unique_lock<std::shared_mutex> writeLock(indexMapMutex_);
    auto files = fs::FileUtils::listAllFilesInDir(annIndexPath_.c_str(), false, nullptr);
    if (files.empty()) {
      LOG(ERROR) << "No existing index files found in " << annIndexPath_;
      return Status::OK();
    }
    for (const auto& filename : files) {
      if (filename.size() > 6 && filename.substr(filename.size() - 6) == ".index") {
        // Parse filename to extract spaceId, partitionId, indexId
        // Format: {spaceId}_{partitionId}_{indexId}.index
        std::string baseName = filename.substr(0, filename.size() - 6);
        std::vector<std::string> parts;
        std::string current;

        for (char c : baseName) {
          if (c == '_') {
            if (!current.empty()) {
              parts.push_back(current);
              current.clear();
            }
          } else {
            current += c;
          }
        }
        if (!current.empty()) {
          parts.push_back(current);
        }

        if (parts.size() != 3) {
          LOG(WARNING) << "Invalid index file name format: " << filename;
          continue;
        }

        try {
          GraphSpaceID spaceId = folly::to<GraphSpaceID>(parts[0]);
          PartitionID partitionId = folly::to<PartitionID>(parts[1]);
          IndexID indexId = folly::to<IndexID>(parts[2]);
          // Try both tag and edge indexes since we don't know the type from filename
          auto tagIndexRet = indexManager_->getTagAnnIndex(spaceId, indexId);
          auto edgeIndexRet = indexManager_->getEdgeAnnIndex(spaceId, indexId);

          std::shared_ptr<meta::cpp2::AnnIndexItem> indexItem;
          if (tagIndexRet.ok() && tagIndexRet.value()) {
            indexItem = tagIndexRet.value();
          } else if (edgeIndexRet.ok() && edgeIndexRet.value()) {
            indexItem = edgeIndexRet.value();
          } else {
            LOG(ERROR) << "Failed to get index metadata for space " << spaceId << ", index "
                       << indexId << ". Tag index result: " << tagIndexRet.status().toString()
                       << ", Edge index result: " << edgeIndexRet.status().toString();
            continue;
          }
          // Create index instance
          auto index = createIndex(spaceId, partitionId, indexId, indexItem);
          if (!index) {
            LOG(ERROR) << "Failed to create index instance for space " << spaceId << ", partition "
                       << partitionId << ", index " << indexId;
            continue;
          }
          // Load index from disk
          std::string fullPath = fs::FileUtils::joinPath(annIndexPath_, filename);
          auto loadStatus = index->read(fullPath);
          if (!loadStatus.ok()) {
            LOG(ERROR) << "Failed to load index from file " << fullPath << ": "
                       << loadStatus.toString();
            continue;
          }
          // Add to index map
          VectorIndexKey key{partitionId, indexId};
          indexMap_[spaceId][key] = index;
        } catch (const std::exception& e) {
          LOG(ERROR) << "Failed to parse index file name " << filename << ": " << e.what();
          continue;
        }
      }
    }

    LOG(INFO) << "Finished loading existing indexes from " << annIndexPath_;
    return Status::OK();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error while loading existing indexes: " << e.what();
    return Status::Error("Failed to load existing indexes: %s", e.what());
  }
}

Status VectorIndexManager::writeAnnIndexesToDisk() {
  std::unique_lock<std::shared_mutex> writeLock(indexMapMutex_);
  for (const auto& spacePair : indexMap_) {
    GraphSpaceID spaceId = spacePair.first;
    for (const auto& indexPair : spacePair.second) {
      const VectorIndexKey& key = indexPair.first;
      const auto& index = indexPair.second;

      // Write each index to its respective file
      std::string filePath = fmt::format("{}_{}_{}.index", spaceId, key.first, key.second);
      auto status = index->write(annIndexPath_, filePath);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to write index to disk: " << status.toString();
        return status;
      }
    }
  }
  LOG(INFO) << "All vector indexes written to disk successfully";
  return Status::OK();
}

}  // namespace storage
}  // namespace nebula
