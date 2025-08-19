/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef COMMON_VECTORINDEX_H_

#define COMMON_VECTORINDEX_H_

#include <common/thrift/ThriftTypes.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/MetricType.h>
#include <faiss/impl/IDSelector.h>
#include <faiss/impl/ProductQuantizer.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>
#include <hnswlib/hnswalg.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>

#include "common/base/Base.h"
#include "common/base/Logging.h"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Vector.h"
#include "common/vectorIndex/VectorIndexUtils.h"

// Forward declarations to avoid circular dependencies
namespace nebula {
/**
 * @brief Interface for vector index, such as HNSW or Faiss.
 */
class AnnIndex {
 public:
  AnnIndex() = default;

  AnnIndex(GraphSpaceID graphID,
           PartitionID partitionID,
           IndexID indexID,
           const std::string &indexName,
           bool propFromNode,
           size_t dim,
           const std::string &rootPath,
           MetricType metricType,
           size_t minTrainDataSize = 3);

  virtual ~AnnIndex() = default;
  AnnIndex(const AnnIndex &) = delete;
  AnnIndex &operator=(const AnnIndex &) = delete;

  [[nodiscard]] virtual Status init(const BuildParams *params) = 0;
  // add data to index incrementally
  [[nodiscard]] virtual Status add(const VecData *data, bool isTest) = 0;
  // upsert data to index
  [[nodiscard]] virtual Status upsert(const VecData *data) = 0;
  // soft delete data from index, return number of deleted vectors
  [[nodiscard]] virtual StatusOr<size_t> remove(const IDSelector &sel) = 0;

  // ann search
  [[nodiscard]] virtual Status search(const SearchParams *params, SearchResult *res) = 0;
  // reconstruct vector by id
  [[nodiscard]] virtual StatusOr<Vector> reconstruct(VectorID id) = 0;

  // load index file from disk
  // flush index to disk
  [[nodiscard]] virtual Status read(const std::string &file) = 0;
  [[nodiscard]] virtual Status write(const std::string &dir, const std::string &file) = 0;

  // apply raft log, disable index dump and roll the WAL when taking snapshot
  [[nodiscard]] virtual Status applyRaftLog(const AnnRaftLog &log, bool takingSnapshot) = 0;
  // load raft log to memory index
  [[nodiscard]] virtual Status loadRaftLog(const AnnRaftLog &log) = 0;

  // create wal file or open existing wal file via file name(not absolute path)
  [[nodiscard]] virtual Status openWal(const std::string &walName) = 0;
  // close wal file and release write/read stream
  [[nodiscard]] virtual Status closeWal() = 0;
  // remove wal file
  [[nodiscard]] virtual Status removeWal() = 0;
  // remove idx file
  [[nodiscard]] virtual Status removeIdxFile() = 0;
  // load index file and wal file from disk
  [[nodiscard]] virtual Status load() = 0;
  // create checkpoint for snapshot
  [[nodiscard]] virtual Status createCheckpoint(const std::string &snapshotPath) = 0;
  // return data counts in ann index
  [[nodiscard]] virtual size_t size() const = 0;

  virtual AnnIndexType indexType() const = 0;
  virtual std::string toString() const = 0;

 protected:
  GraphSpaceID graphID_;
  PartitionID partitionID_;
  IndexID indexID_;
  std::string indexName_;
  bool propFromNode_;
  size_t dim_;
  std::string rootPath_;
  MetricType metricType_;
  size_t minTrainDataSize_{3};       // minimum training data size for IVF index
  mutable std::shared_mutex latch_;  // for thread safety
};

}  // namespace nebula
#endif  // COMMON_VECTORINDEX_H_
