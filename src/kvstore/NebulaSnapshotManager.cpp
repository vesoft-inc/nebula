/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/NebulaSnapshotManager.h"

#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/RateLimiter.h"

DEFINE_uint32(snapshot_part_rate_limit,
              1024 * 1024 * 2,
              "max bytes of pulling snapshot for each partition in one second");
DEFINE_uint32(snapshot_batch_size, 1024 * 512, "batch size for snapshot, in bytes");

namespace nebula {
namespace kvstore {

const int32_t kReserveNum = 1024 * 4;

NebulaSnapshotManager::NebulaSnapshotManager(NebulaStore* kv) : store_(kv) {
  // Snapshot rate is limited to FLAGS_snapshot_worker_threads * FLAGS_snapshot_part_rate_limit.
  // So by default, the total send rate is limited to 4 * 2Mb = 8Mb.
  rateLimiter_.reset(
      new RateLimiter(FLAGS_snapshot_part_rate_limit, FLAGS_snapshot_part_rate_limit));
}

void NebulaSnapshotManager::accessAllRowsInSnapshot(GraphSpaceID spaceId,
                                                    PartitionID partId,
                                                    raftex::SnapshotCallback cb) {
  CHECK_NOTNULL(store_);
  auto tables = NebulaKeyUtils::snapshotPrefix(partId);
  std::vector<std::string> data;
  int64_t totalSize = 0;
  int64_t totalCount = 0;
  LOG(INFO) << folly::format(
      "Space {} Part {} start send snapshot, rate limited to {}, batch size is {}",
      spaceId,
      partId,
      FLAGS_snapshot_part_rate_limit,
      FLAGS_snapshot_batch_size);
  // raft will make sure that there won't be concurrent accessAllRowsInSnapshot of a given partition
  rateLimiter_->add(spaceId, partId);
  SCOPE_EXIT { rateLimiter_->remove(spaceId, partId); };

  for (const auto& prefix : tables) {
    if (!accessTable(spaceId, partId, prefix, cb, data, totalCount, totalSize)) {
      return;
    }
  }
  cb(data, totalCount, totalSize, raftex::SnapshotStatus::DONE);
}

// Promise is set in callback. Access part of the data, and try to send to
// peers. If send failed, will return false.
bool NebulaSnapshotManager::accessTable(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const std::string& prefix,
                                        raftex::SnapshotCallback& cb,
                                        std::vector<std::string>& data,
                                        int64_t& totalCount,
                                        int64_t& totalSize) {
  std::unique_ptr<KVIterator> iter;
  auto ret = store_->prefix(spaceId, partId, prefix, &iter);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "[spaceId:" << spaceId << ", partId:" << partId << "] access prefix failed"
              << ", error code:" << static_cast<int32_t>(ret);
    cb(data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
    return false;
  }
  data.reserve(kReserveNum);
  size_t batchSize = 0;
  while (iter && iter->valid()) {
    if (batchSize >= FLAGS_snapshot_batch_size) {
      rateLimiter_->consume(spaceId, partId, batchSize);
      if (cb(data, totalCount, totalSize, raftex::SnapshotStatus::IN_PROGRESS)) {
        data.clear();
        batchSize = 0;
      } else {
        LOG(INFO) << "[spaceId:" << spaceId << ", partId:" << partId << "] send snapshot failed";
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
