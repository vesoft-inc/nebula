/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/NebulaSnapshotManager.h"

#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/RateLimiter.h"

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
  static constexpr LogID kInvalidLogId = -1;
  static constexpr TermID kInvalidLogTerm = -1;
  std::vector<std::string> data;
  int64_t totalSize = 0;
  int64_t totalCount = 0;
  CHECK_NOTNULL(store_);
  auto partRet = store_->part(spaceId, partId);
  if (!ok(partRet)) {
    LOG(INFO) << folly::sformat("Failed to find space {} part {]", spaceId, partId);
    cb(kInvalidLogId, kInvalidLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
    return;
  }
  // Create a rocksdb snapshot
  auto snapshot = store_->GetSnapshot(spaceId, partId);
  SCOPE_EXIT {
    if (snapshot != nullptr) {
      store_->ReleaseSnapshot(spaceId, partId, snapshot);
    }
  };
  auto part = nebula::value(partRet);
  // Get the commit log id and commit log term of specified partition
  std::string val;
  auto commitRet = part->engine()->get(NebulaKeyUtils::systemCommitKey(partId), &val, snapshot);
  if (commitRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << folly::sformat(
        "Cannot fetch the commit log id and term of space {} part {}", spaceId, partId);
    cb(kInvalidLogId, kInvalidLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
    return;
  }
  CHECK_EQ(val.size(), sizeof(LogID) + sizeof(TermID));
  LogID commitLogId;
  TermID commitLogTerm;
  memcpy(reinterpret_cast<void*>(&commitLogId), val.data(), sizeof(LogID));
  memcpy(reinterpret_cast<void*>(&commitLogTerm), val.data() + sizeof(LogID), sizeof(TermID));

  LOG(INFO) << folly::sformat(
      "Space {} Part {} start send snapshot of commitLogId {} commitLogTerm {}, rate limited to "
      "{}, batch size is {}",
      spaceId,
      partId,
      commitLogId,
      commitLogTerm,
      FLAGS_snapshot_part_rate_limit,
      FLAGS_snapshot_batch_size);

  auto rateLimiter = std::make_unique<kvstore::RateLimiter>();
  auto tables = NebulaKeyUtils::snapshotPrefix(partId);
  for (const auto& prefix : tables) {
    if (!accessTable(spaceId,
                     partId,
                     snapshot,
                     prefix,
                     cb,
                     commitLogId,
                     commitLogTerm,
                     data,
                     totalCount,
                     totalSize,
                     rateLimiter.get())) {
      return;
    }
  }
  cb(commitLogId, commitLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::DONE);
}

// Promise is set in callback. Access part of the data, and try to send to
// peers. If send failed, will return false.
bool NebulaSnapshotManager::accessTable(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const void* snapshot,
                                        const std::string& prefix,
                                        raftex::SnapshotCallback& cb,
                                        LogID commitLogId,
                                        TermID commitLogTerm,
                                        std::vector<std::string>& data,
                                        int64_t& totalCount,
                                        int64_t& totalSize,
                                        kvstore::RateLimiter* rateLimiter) {
  std::unique_ptr<KVIterator> iter;
  auto ret = store_->prefix(spaceId, partId, prefix, &iter, false, snapshot);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "[spaceId:" << spaceId << ", partId:" << partId << "] access prefix failed"
            << ", error code:" << static_cast<int32_t>(ret);
    cb(commitLogId, commitLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
    return false;
  }
  data.reserve(kReserveNum);
  size_t batchSize = 0;
  while (iter && iter->valid()) {
    if (batchSize >= FLAGS_snapshot_batch_size) {
      rateLimiter->consume(static_cast<double>(batchSize),                        // toConsume
                           static_cast<double>(FLAGS_snapshot_part_rate_limit),   // rate
                           static_cast<double>(FLAGS_snapshot_part_rate_limit));  // burstSize
      if (cb(commitLogId,
             commitLogTerm,
             data,
             totalCount,
             totalSize,
             raftex::SnapshotStatus::IN_PROGRESS)) {
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
