/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/NebulaSnapshotManager.h"

#include <folly/Format.h>      // for sformat
#include <folly/Function.h>    // for FunctionTraits
#include <folly/ScopeGuard.h>  // for operator+, SCOPE_EXIT
#include <gflags/gflags.h>     // for DEFINE_uint32
#include <stddef.h>            // for size_t

#include <algorithm>  // for max
#include <memory>     // for unique_ptr, make_unique

#include "common/base/Logging.h"              // for LogMessage, CheckNotNull
#include "common/utils/NebulaKeyUtils.h"      // for NebulaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::S...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "kvstore/LogEncoder.h"               // for encodeKV
#include "kvstore/NebulaStore.h"              // for NebulaStore
#include "kvstore/RateLimiter.h"              // for RateLimiter

DEFINE_uint32(snapshot_part_rate_limit,
              1024 * 1024 * 10,
              "max bytes of pulling snapshot for each partition in one second");
DEFINE_uint32(snapshot_batch_size, 1024 * 512, "batch size for snapshot, in bytes");

namespace nebula {
namespace kvstore {

const int32_t kReserveNum = 1024 * 4;

NebulaSnapshotManager::NebulaSnapshotManager(NebulaStore* kv) : store_(kv) {
  // Snapshot rate is limited to FLAGS_snapshot_worker_threads * FLAGS_snapshot_part_rate_limit.
  // So by default, the total send rate is limited to 4 * 10Mb = 40Mb.
  LOG(INFO) << "Send snapshot is rate limited to " << FLAGS_snapshot_part_rate_limit
            << " for each part by default";
}

void NebulaSnapshotManager::accessAllRowsInSnapshot(GraphSpaceID spaceId,
                                                    PartitionID partId,
                                                    raftex::SnapshotCallback cb) {
  auto rateLimiter = std::make_unique<kvstore::RateLimiter>();
  CHECK_NOTNULL(store_);
  auto tables = NebulaKeyUtils::snapshotPrefix(partId);
  std::vector<std::string> data;
  int64_t totalSize = 0;
  int64_t totalCount = 0;
  LOG(INFO) << folly::sformat(
      "Space {} Part {} start send snapshot, rate limited to {}, batch size is {}",
      spaceId,
      partId,
      FLAGS_snapshot_part_rate_limit,
      FLAGS_snapshot_batch_size);

  const void* snapshot = store_->GetSnapshot(spaceId, partId);
  SCOPE_EXIT {
    if (snapshot != nullptr) {
      store_->ReleaseSnapshot(spaceId, partId, snapshot);
    }
  };

  for (const auto& prefix : tables) {
    if (!accessTable(spaceId,
                     partId,
                     snapshot,
                     prefix,
                     cb,
                     data,
                     totalCount,
                     totalSize,
                     rateLimiter.get())) {
      return;
    }
  }
  cb(data, totalCount, totalSize, raftex::SnapshotStatus::DONE);
}

// Promise is set in callback. Access part of the data, and try to send to
// peers. If send failed, will return false.
bool NebulaSnapshotManager::accessTable(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const void* snapshot,
                                        const std::string& prefix,
                                        raftex::SnapshotCallback& cb,
                                        std::vector<std::string>& data,
                                        int64_t& totalCount,
                                        int64_t& totalSize,
                                        kvstore::RateLimiter* rateLimiter) {
  std::unique_ptr<KVIterator> iter;
  auto ret = store_->prefix(spaceId, partId, prefix, &iter, false, snapshot);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "[spaceId:" << spaceId << ", partId:" << partId << "] access prefix failed"
            << ", error code:" << static_cast<int32_t>(ret);
    cb(data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
    return false;
  }
  data.reserve(kReserveNum);
  size_t batchSize = 0;
  while (iter && iter->valid()) {
    if (batchSize >= FLAGS_snapshot_batch_size) {
      rateLimiter->consume(static_cast<double>(batchSize),                        // toConsume
                           static_cast<double>(FLAGS_snapshot_part_rate_limit),   // rate
                           static_cast<double>(FLAGS_snapshot_part_rate_limit));  // burstSize
      if (cb(data, totalCount, totalSize, raftex::SnapshotStatus::IN_PROGRESS)) {
        data.clear();
        batchSize = 0;
      } else {
        VLOG(2) << "[spaceId:" << spaceId << ", partId:" << partId << "] send snapshot failed";
        return false;
      }
    }
    auto key = iter->key();
    auto val = iter->val();
    data.emplace_back(encodeKV(key, val));
    batchSize += data.back().size();
    totalSize += data.back().size();
    totalCount++;
    iter->next();
  }
  return true;
}

}  // namespace kvstore
}  // namespace nebula
