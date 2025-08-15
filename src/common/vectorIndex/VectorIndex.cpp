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
      minTrainDataSize_(minTrainDataSize) {}

}  // namespace nebula
