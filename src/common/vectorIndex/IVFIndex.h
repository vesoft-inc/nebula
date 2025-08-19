/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef COMMON_VECTORINDEX_IVF_INDEX_H_
#define COMMON_VECTORINDEX_IVF_INDEX_H_

#include "common/vectorIndex/VectorIndex.h"
namespace nebula {
class IVFIndex : public AnnIndex {
 public:
  IVFIndex(GraphSpaceID graphID,
           PartitionID partitionID,
           IndexID indexID,
           const std::string &indexName,
           bool propFromNode,
           size_t dim,
           const std::string &rootPath,
           MetricType metricType,
           size_t minTrainDataSize = 3);

  [[nodiscard]] Status init(const BuildParams *params) override;
  // add data to index incrementally
  [[nodiscard]] Status add(const VecData *data, bool isTest = false) override;
  // upsert data to index
  [[nodiscard]] Status upsert(const VecData *data) override;
  // soft delete data from index, return number of deleted vectors
  [[nodiscard]] StatusOr<size_t> remove(const IDSelector &sel) override;

  // ann search
  [[nodiscard]] Status search(const SearchParams *params, SearchResult *res) override;
  // reconstruct vector by id
  [[nodiscard]] StatusOr<Vector> reconstruct(VectorID id) override;

  // load index file from disk
  // flush index to disk
  [[nodiscard]] Status read(const std::string &file) override;
  [[nodiscard]] Status write(const std::string &dir, const std::string &file) override;

  // apply raft log, disable index dump and roll the WAL when taking snapshot
  [[nodiscard]] Status applyRaftLog(const AnnRaftLog &log, bool takingSnapshot) override;
  // load raft log to memory index
  [[nodiscard]] Status loadRaftLog(const AnnRaftLog &log) override;

  // create wal file or open existing wal file via file name(not absolute path)
  [[nodiscard]] Status openWal(const std::string &walName) override;
  // close wal file and release write/read stream
  [[nodiscard]] Status closeWal() override;
  // remove wal file
  [[nodiscard]] Status removeWal() override;
  // remove idx file
  [[nodiscard]] Status removeIdxFile() override;
  // load index file and wal file from disk
  [[nodiscard]] Status load() override;
  // create checkpoint for snapshot
  [[nodiscard]] Status createCheckpoint(const std::string &snapshotPath) override;
  // return data counts in ann index
  [[nodiscard]] size_t size() const override;

  AnnIndexType indexType() const override;
  std::string toString() const override;

 private:
  size_t nlist_;
  size_t nprobe_;
  size_t trainsz_;
  std::unique_ptr<faiss::Index> ivfIndex_{nullptr};
  static constexpr size_t STACK_THRESHOLD = 64;
};
}  // namespace nebula

#endif
