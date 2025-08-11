/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/vectorIndex/IVFIndex.h"

#include <faiss/Index.h>
#include <faiss/IndexIVF.h>
#include <faiss/MetricType.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>
#include <faiss/invlists/DirectMap.h>

namespace nebula {
IVFIndex::IVFIndex(GraphSpaceID graphID,
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
      nlist_(0),
      nprobe_(0),
      trainsz_(0) {}

[[nodiscard]] Status IVFIndex::init(const BuildParams *params) {
  if (params == nullptr) {
    return Status::Error("BuildParams is null");
  }
  if (params->indexType != AnnIndexType::IVF) {
    return Status::Error("BuildParams indexType is not IVF");
  }
  const auto *ivfParams = dynamic_cast<const BuildParamsIVF *>(params);
  if (ivfParams == nullptr) {
    return Status::Error("BuildParams is not BuildParamsIVF");
  }
  nlist_ = ivfParams->nl;
  trainsz_ = ivfParams->ts;
  if (nlist_ < 1 || trainsz_ < 1) {
    return Status::Error("Invalid nlist or trainsz");
  }
  if (trainsz_ < minTrainDataSize_) {
    return Status::Error("Training data size is less than minimum required size");
  }
  std::string description;
  if (metricType_ == MetricType::L2) {
    description = "IVF" + std::to_string(nlist_) + ",Flat";
    auto index = faiss::index_factory(dim_, description.c_str(), faiss::METRIC_L2);
    ivfIndex_.reset(dynamic_cast<faiss::IndexIVFFlat *>(index));
  } else if (metricType_ == MetricType::INNER_PRODUCT) {
    description = "IVF" + std::to_string(nlist_) + ",Flat";
    auto index = faiss::index_factory(dim_, description.c_str(), faiss::METRIC_INNER_PRODUCT);
    ivfIndex_.reset(dynamic_cast<faiss::IndexIVFFlat *>(index));
  } else {
    return Status::Error("Unsupported metric type");
  }

  // Enable direct map so that reconstruct(id) and reconstruct_batch() work with external IDs
  if (ivfIndex_ != nullptr) {
    auto *ivf = dynamic_cast<faiss::IndexIVF *>(ivfIndex_.get());
    if (ivf != nullptr) {
      ivf->set_direct_map_type(faiss::DirectMap::Hashtable);
    }
  }
  return Status::OK();
}

// add data to index incrementally
[[nodiscard]] Status IVFIndex::add(const VecData *data) {
  if (data == nullptr) {
    return Status::Error("VecData is null");
  }
  if (data->cnt < 1 || data->cnt < trainsz_ || data->dim != dim_) {
    return Status::Error("Invalid VecData");
  }
  if (ivfIndex_ == nullptr) {
    return Status::Error("IVF index is not initialized");
  }
  std::unique_lock lock(latch_);
  // first train ivfflat index
  try {
    ivfIndex_->train(trainsz_, data->fdata);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to train IVF index: " << e.what();
    return Status::Error("Failed to train IVF index");
  }
  // then add data to index
  try {
    ivfIndex_->add_with_ids(data->cnt - trainsz_, data->fdata + trainsz_ * dim_, data->ids);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to add data to IVF index: " << e.what();
    return Status::Error("Failed to add data to IVF index");
  }
  return Status::OK();
}

// upsert data to index
[[nodiscard]] Status IVFIndex::upsert(const VecData *data) {
  if (data == nullptr) {
    return Status::Error("VecData is null");
  }
  if (data->cnt < 1 || data->dim != dim_) {
    return Status::Error("Invalid VecData");
  }
  if (ivfIndex_ == nullptr) {
    return Status::Error("IVF index is not initialized");
  }
  // upsert data to index: remove then insert
  // With DirectMap::Hashtable, FAISS requires IDSelectorArray for remove_ids
  faiss::IDSelectorArray idsToRemove(data->cnt, data->ids);
  try {
    std::unique_lock lock(latch_);
    ivfIndex_->remove_ids(idsToRemove);
    ivfIndex_->add_with_ids(data->cnt, data->fdata, data->ids);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to upsert data to IVF index: " << e.what();
    return Status::Error("Failed to upsert data to IVF index");
  }
  return Status::OK();
}

// soft delete data from index, return number of deleted vectors
[[nodiscard]] StatusOr<size_t> IVFIndex::remove(const IDSelector &sel) {
  if (ivfIndex_ == nullptr) {
    return Status::Error("IVF index is not initialized");
  }
  // remove ids from index
  // With DirectMap::Hashtable, FAISS requires IDSelectorArray for remove_ids
  faiss::IDSelectorArray idsToRemove(sel.cnt, sel.ids);
  size_t removedCount = 0;
  try {
    std::unique_lock lock(latch_);
    removedCount = ivfIndex_->remove_ids(idsToRemove);
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to remove ids from IVF index: " << e.what();
    return Status::Error("Failed to remove ids from IVF index");
  }
  return removedCount;
}

// ann search
[[nodiscard]] Status IVFIndex::search(const SearchParams *params, SearchResult *res) {
  if (params == nullptr || res == nullptr) {
    return Status::Error("SearchParams or SearchResult is null");
  }
  if (ivfIndex_ == nullptr) {
    return Status::Error("IVF index is not initialized");
  }
  if (params->topK < 1 || params->query == nullptr || params->queryDim != dim_) {
    return Status::Error("Invalid SearchParams");
  }
  const auto *ivfParams = dynamic_cast<const SearchParamsIVF *>(params);
  if (ivfParams == nullptr) {
    return Status::Error("SearchParams is not SearchParamsIVF");
  }
  if (ivfParams->nprobe < 1) {
    return Status::Error("Invalid nprobe");
  }

  // set nprobe
  static_cast<faiss::IndexIVF *>(ivfIndex_.get())->nprobe = ivfParams->nprobe;
  // search
  size_t k = params->topK;
  res->IDs.resize(k);
  res->distances.resize(k);
  try {
    std::shared_lock lock(latch_);
    ivfIndex_->search(1, params->query, k, res->distances.data(), res->IDs.data());
    // FAISS may return -1 for missing results
    res->vectors.resize(k * dim_);
    ivfIndex_->reconstruct_batch(k, res->IDs.data(), res->vectors.data());
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to search IVF index: " << e.what();
    return Status::Error("Failed to search IVF index");
  }
  return Status::OK();
}

// reconstruct vector by id
[[nodiscard]] StatusOr<Vector> IVFIndex::reconstruct(VectorID id) {
  if (ivfIndex_ == nullptr) {
    return Status::Error("IVF index is not initialized");
  }
  if (id < 0) {
    return Status::Error("Invalid vector id");
  }
  std::vector<float> vec(dim_);
  try {
    std::shared_lock lock(latch_);
    ivfIndex_->reconstruct(id, vec.data());
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to reconstruct vector from IVF index: " << e.what();
    return Status::Error("Failed to reconstruct vector from IVF index");
  }
  return Vector(std::move(vec));
}

// load index file from disk
// flush index to disk
[[nodiscard]] Status IVFIndex::read(const std::string &file) {
  if (ivfIndex_ == nullptr) {
    return Status::Error("IVF index is not initialized");
  }
  faiss::Index *index;
  try {
    std::unique_lock lock(latch_);
    index = faiss::read_index(file.c_str());
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to read IVF index from file: " << e.what();
    return Status::Error("Failed to read IVF index from file");
  }
  if (index == nullptr) {
    return Status::Error("Failed to read IVF index from file");
  }
  ivfIndex_.reset(dynamic_cast<faiss::IndexIVFFlat *>(index));
  return Status::OK();
}
[[nodiscard]] Status IVFIndex::write(const std::string &dir, const std::string &file) {
  if (ivfIndex_ == nullptr) {
    return Status::Error("IVF index is not initialized");
  }
  std::string fullPath = dir + "/" + file;
  try {
    std::unique_lock lock(latch_);
    faiss::write_index(ivfIndex_.get(), fullPath.c_str());
  } catch (const std::exception &e) {
    LOG(ERROR) << "Failed to write IVF index to file: " << e.what();
    return Status::Error("Failed to write IVF index to file");
  }
  return Status::OK();
}

// apply raft log, disable index dump and roll the WAL when taking snapshot
[[nodiscard]] Status IVFIndex::applyRaftLog(const AnnRaftLog &log, bool takingSnapshot) {
  UNUSED(log);
  UNUSED(takingSnapshot);
  return Status::OK();
}
// load raft log to memory index
[[nodiscard]] Status IVFIndex::loadRaftLog(const AnnRaftLog &log) {
  UNUSED(log);
  return Status::OK();
}

// create wal file or open existing wal file via file name(not absolute path)
[[nodiscard]] Status IVFIndex::openWal(const std::string &walName) {
  UNUSED(walName);
  return Status::OK();
}
// close wal file and release write/read stream
[[nodiscard]] Status IVFIndex::closeWal() {
  return Status::OK();
}
// remove wal file
[[nodiscard]] Status IVFIndex::removeWal() {
  return Status::OK();
}
// remove idx file
[[nodiscard]] Status IVFIndex::removeIdxFile() {
  return Status::OK();
}
// load index file and wal file from disk
[[nodiscard]] Status IVFIndex::load() {
  return Status::OK();
}
// create checkpoint for snapshot
[[nodiscard]] Status IVFIndex::createCheckpoint(const std::string &snapshotPath) {
  UNUSED(snapshotPath);
  return Status::OK();
}

AnnIndexType IVFIndex::indexType() const {
  return AnnIndexType::IVF;
}
std::string IVFIndex::toString() const {
  std::string description = "IVFIndex ( GraphID: " + std::to_string(graphID_) +
                            ", Part: " + std::to_string(partitionID_) +
                            ", IndexID: " + std::to_string(indexID_) +
                            ", IndexName: " + indexName_ + "\n ";
  description += "Dim: " + std::to_string(dim_) + ", Rootpath: " + rootPath_ +
                 ", nlist: " + std::to_string(nlist_) +
                 ", train size: " + std::to_string(trainsz_) +
                 ", nprobe: " + std::to_string(nprobe_) + ")";
  return description;
}

}  // namespace nebula
