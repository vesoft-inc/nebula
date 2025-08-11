/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/vectorIndex/HNSWIndex.h"

#include <hnswlib/space_l2.h>

#include <filesystem>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/vectorIndex/VectorIndexUtils.h"

namespace nebula {
HNSWIndex::HNSWIndex(GraphSpaceID graphID,
                     PartitionID partitionID,
                     IndexID indexID,
                     const std::string &indexName,
                     bool propFromNode,
                     size_t dim,
                     const std::string &rootPath,
                     MetricType metricType,
                     size_t minTrainDataSize)
    : AnnIndex(graphID,
               partitionID,
               indexID,
               indexName,
               propFromNode,
               dim,
               rootPath,
               metricType,
               minTrainDataSize),
      M_(0),
      efConstruction_(0),
      maxElements_(0),
      efSearch_(0) {}

[[nodiscard]] Status HNSWIndex::init(const BuildParams *params) {
  if (params == nullptr) {
    return Status::Error("BuildParams is null");
  }
  if (params->indexType != AnnIndexType::HNSW) {
    return Status::Error("BuildParams indexType is not HNSW");
  }
  const auto *hnswParams = dynamic_cast<const BuildParamsHNSW *>(params);
  if (hnswParams == nullptr) {
    return Status::Error("BuildParams is not BuildParamsHNSW");
  }
  M_ = hnswParams->maxDegree;
  efConstruction_ = hnswParams->efConstruction;
  maxElements_ = hnswParams->capacity;
  LOG(INFO) << "HNSW init params - M: " << M_ << ", efConstruction: " << efConstruction_
            << ", maxElements: " << maxElements_;
  if (hnswParams->metricType == MetricType::L2) {
    space_ = std::make_unique<hnswlib::L2Space>(dim_);
    rawHnsw_ = std::make_unique<hnswlib::HierarchicalNSW<float>>(
        space_.get(), maxElements_, M_, efConstruction_);
  } else if (hnswParams->metricType == MetricType::INNER_PRODUCT) {
    space_ = std::make_unique<hnswlib::InnerProductSpace>(dim_);
    rawHnsw_ = std::make_unique<hnswlib::HierarchicalNSW<float>>(
        space_.get(), maxElements_, M_, efConstruction_);
  } else {
    return Status::Error("Unsupported metric type for HNSW index");
  }

  return Status::OK();
}

// add data to index incrementally
[[nodiscard]] Status HNSWIndex::add(const VecData *data) {
  if (data == nullptr) {
    return Status::Error("VecData is null");
  }
  if (data->cnt < 1 || data->dim != dim_) {
    return Status::Error("Invalid VecData");
  }
  if (rawHnsw_ == nullptr) {
    return Status::Error("HNSW index is not initialized");
  }

  size_t cnt = data->cnt;
  std::unique_lock lock(latch_);
  try {
    // Add data to index
#pragma omp parallel for schedule(static)
    for (size_t i = 0; i < cnt; ++i) {
      rawHnsw_->addPoint(data->fdata + i * dim_, data->ids[i]);
    }
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to add data to HNSW index: " << e.what();
    return Status::Error("Failed to add data to HNSW index");
  }
  return Status::OK();
}

// upsert data to index
[[nodiscard]] Status HNSWIndex::upsert(const VecData *data) {
  if (data == nullptr) {
    return Status::Error("VecData is null");
  }
  if (data->cnt < 1 || data->dim != dim_) {
    return Status::Error("Invalid VecData");
  }
  if (rawHnsw_ == nullptr) {
    return Status::Error("HNSW index is not initialized");
  }

  // upsert data to index
  std::unique_lock lock(latch_);

  try {
#pragma omp parallel for schedule(static)
    for (size_t i = 0; i < data->cnt; ++i) {
      // Adds point. Updates the point if it is already in the index.
      rawHnsw_->addPoint(data->fdata + i * dim_, data->ids[i]);
    }
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to upsert data to HNSW index: " << e.what();
    return Status::Error("Failed to upsert data to HNSW index");
  }
  return Status::OK();
}

// soft delete data from index, return number of deleted vectors
[[nodiscard]] StatusOr<size_t> HNSWIndex::remove(const IDSelector &sel) {
  if (rawHnsw_ == nullptr) {
    return Status::Error("HNSW index is not initialized");
  }

  std::atomic<size_t> removedCount = 0;
  std::unique_lock lock(latch_);

  try {
#pragma omp parallel for schedule(static)
    for (size_t i = 0; i < sel.cnt; ++i) {
      if (sel.ids[i] < 0) {
        continue;  // skip invalid ids
      }
      rawHnsw_->markDelete(sel.ids[i]);
      removedCount++;
    }
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to remove ids from HNSW index: " << e.what();
    return Status::Error("Failed to remove ids from HNSW index");
  }
  return removedCount;
}

// ann search
[[nodiscard]] Status HNSWIndex::search(const SearchParams *params, SearchResult *res) {
  if (params == nullptr || res == nullptr) {
    return Status::Error("SearchParams or SearchResult is null");
  }
  if (rawHnsw_ == nullptr) {
    return Status::Error("HNSW index is not initialized");
  }
  if (params->topK < 1 || params->query == nullptr || params->queryDim != dim_) {
    return Status::Error("Invalid SearchParams");
  }
  const auto *hnswParams = dynamic_cast<const SearchParamsHNSW *>(params);
  if (hnswParams == nullptr) {
    return Status::Error("SearchParams is not SearchParamsHNSW");
  }
  efSearch_ = hnswParams->efSearch;
  size_t k = params->topK;
  res->IDs.resize(k);
  res->distances.resize(k);
  res->vectors.resize(k * dim_);

  std::priority_queue<std::pair<float, size_t>> results;
  try {
    std::shared_lock lock(latch_);
    // Set ef parameter for search
    rawHnsw_->setEf(efSearch_);
    results = rawHnsw_->searchKnn(params->query, k);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to search HNSW index: " << e.what();
    return Status::Error("Failed to search HNSW index");
  }
  res->IDs.clear();
  res->distances.clear();
  res->vectors.clear();
  if (results.empty()) {
    LOG(WARNING) << "No results found for query in HNSW index";
    return Status::OK();  // No results, return empty result
  }

  // fill the results
  size_t count = 0;
  while (!results.empty()) {
    auto result = results.top();
    results.pop();
    if (count++ < k) {
      res->IDs.emplace_back(result.second);
      res->distances.emplace_back(result.first);
      // reconstruct the vector
      auto vec = rawHnsw_->getDataByLabel<float>(result.second);
      res->vectors.insert(res->vectors.end(), vec.begin(), vec.end());
    } else {
      break;  // stop when we have enough results
    }
  }
  return Status::OK();
}

// reconstruct vector by id
[[nodiscard]] StatusOr<Vector> HNSWIndex::reconstruct(VectorID id) {
  if (rawHnsw_ == nullptr) {
    return Status::Error("HNSW index is not initialized");
  }
  if (id < 0) {
    return Status::Error("Invalid vector id");
  }
  std::vector<float> vec;
  try {
    std::shared_lock lock(latch_);
    vec = rawHnsw_->getDataByLabel<float>(id);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to reconstruct vector from HNSW index: " << e.what();
    return Status::Error("Failed to reconstruct vector from HNSW index");
  }
  return Vector(std::move(vec));
}

// load index file from disk
// flush index to disk
[[nodiscard]] Status HNSWIndex::read(const std::string &file) {
  if (rawHnsw_ == nullptr) {
    return Status::Error("HNSW index is not initialized");
  }
  if (metricType_ == MetricType::L2) {
    space_ = std::make_unique<hnswlib::L2Space>(dim_);
  } else if (metricType_ == MetricType::INNER_PRODUCT) {
    space_ = std::make_unique<hnswlib::InnerProductSpace>(dim_);
  } else {
    return Status::Error("Unsupported metric type for HNSW index");
  }

  auto *index = new hnswlib::HierarchicalNSW<float>(space_.get());
  try {
    std::unique_lock lock(latch_);
    index->loadIndex(file, space_.get(), maxElements_);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to read HNSW index from file: " << e.what();
    return Status::Error("Failed to read HNSW index from file");
  }
  rawHnsw_.reset(index);
  return Status::OK();
}

[[nodiscard]] Status HNSWIndex::write(const std::string &dir, const std::string &file) {
  if (rawHnsw_ == nullptr) {
    return Status::Error("HNSW index is not initialized");
  }
  std::string fullPath = dir + "/" + file;
  try {
    std::unique_lock lock(latch_);
    rawHnsw_->saveIndex(fullPath.c_str());
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to write HNSW index to file: " << e.what();
    return Status::Error("Failed to write HNSW index to file");
  }
  return Status::OK();
}

// apply raft log, disable index dump and roll the WAL when taking snapshot
[[nodiscard]] Status HNSWIndex::applyRaftLog(const AnnRaftLog &log, bool takingSnapshot) {
  UNUSED(log);
  UNUSED(takingSnapshot);
  return Status::OK();
}

// load raft log to memory index
[[nodiscard]] Status HNSWIndex::loadRaftLog(const AnnRaftLog &log) {
  UNUSED(log);
  return Status::OK();
}

// create wal file or open existing wal file via file name(not absolute path)
[[nodiscard]] Status HNSWIndex::openWal(const std::string &walName) {
  UNUSED(walName);
  return Status::OK();
}

// close wal file and release write/read stream
[[nodiscard]] Status HNSWIndex::closeWal() {
  return Status::OK();
}

// remove wal file
[[nodiscard]] Status HNSWIndex::removeWal() {
  return Status::OK();
}

// remove idx file
[[nodiscard]] Status HNSWIndex::removeIdxFile() {
  return Status::OK();
}

// load index file and wal file from disk
[[nodiscard]] Status HNSWIndex::load() {
  return Status::OK();
}

// create checkpoint for snapshot
[[nodiscard]] Status HNSWIndex::createCheckpoint(const std::string &snapshotPath) {
  UNUSED(snapshotPath);
  return Status::OK();
}

AnnIndexType HNSWIndex::indexType() const {
  return AnnIndexType::HNSW;
}
std::string HNSWIndex::toString() const {
  std::string description = "HNSW";
  description += "( Graph: " + std::to_string(graphID_) +
                 ", Part: " + std::to_string(partitionID_) +
                 ", IndexID: " + std::to_string(indexID_) + ", IndexName: " + indexName_ + "\n";
  description += "Dim: " + std::to_string(dim_) + ", M: " + std::to_string(M_) +
                 ", efConstruction: " + std::to_string(efConstruction_) +
                 ", maxElements: " + std::to_string(maxElements_) +
                 ", efSearch: " + std::to_string(efSearch_) + ", RootPath: " + rootPath_ + " )";
  description += "\n";

  return description;
}

}  // namespace nebula
