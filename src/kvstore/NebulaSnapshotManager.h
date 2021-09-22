/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/TokenBucket.h>

#include "common/base/Base.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/RateLimiter.h"
#include "kvstore/raftex/SnapshotManager.h"

namespace nebula {
namespace kvstore {

class NebulaSnapshotManager : public raftex::SnapshotManager {
 public:
  explicit NebulaSnapshotManager(NebulaStore* kv);

  void accessAllRowsInSnapshot(GraphSpaceID spaceId,
                               PartitionID partId,
                               raftex::SnapshotCallback cb) override;

 private:
  bool accessTable(GraphSpaceID spaceId,
                   PartitionID partId,
                   const std::string& prefix,
                   raftex::SnapshotCallback& cb,
                   std::vector<std::string>& data,
                   int64_t& totalCount,
                   int64_t& totalSize,
                   kvstore::RateLimiter* rateLimiter);

  NebulaStore* store_;
};

}  // namespace kvstore
}  // namespace nebula
