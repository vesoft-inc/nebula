/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/vectorIndex/VectorIndex.h"

#include "kvstore/wal/FileBasedWal.h"

namespace nebula {

AnnIndex::AnnIndex(GraphSpaceID graphID,
                   PartitionID partitionID,
                   IndexID indexID,
                   const std::string &indexName,
                   bool propFromNode,
                   size_t dim,
                   const std::string &rootPath,
                   MetricType metricType,
                   size_t minTrainDataSize)
    : graphID_(graphID),
      partitionID_(partitionID),
      indexID_(indexID),
      indexName_(indexName),
      propFromNode_(propFromNode),
      dim_(dim),
      rootPath_(rootPath),
      metricType_(metricType),
      minTrainDataSize_(minTrainDataSize) {
  // WAL manager will be set later via setVectorWalManager
}

void AnnIndex::setRaftWal(std::shared_ptr<nebula::wal::FileBasedWal> raftWal) {
  raftWal_ = raftWal;
  LOG(INFO) << "AnnIndex set Raft WAL for index " << indexID_ << " in space " << graphID_
            << " partition " << partitionID_;
}

void AnnIndex::setVectorWalManager(std::shared_ptr<vector::VectorIndexWalManager> walManager) {
  walManager_ = walManager;
  LOG(INFO) << "AnnIndex set Vector WAL Manager for index " << indexID_ << " in space " << graphID_
            << " partition " << partitionID_;
}

std::shared_ptr<vector::VectorIndexWalManager> AnnIndex::getVectorWalManager() const {
  return walManager_;
}

}  // namespace nebula
