/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "kvstore/SnapshotManagerImpl.h"
#include "utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"

DEFINE_int32(snapshot_batch_size, 1024 * 1024 * 10, "batch size for snapshot");

namespace nebula {
namespace kvstore {
const int32_t kReserveNum = 1024 * 4;
void SnapshotManagerImpl::accessAllRowsInSnapshot(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  raftex::SnapshotCallback cb) {
    CHECK_NOTNULL(store_);
    std::unique_ptr<KVIterator> iter;
    auto prefix = NebulaKeyUtils::snapshotPrefix(partId);
    std::vector<std::string> data;
    int64_t totalSize = 0;
    int64_t totalCount = 0;
    auto ret = store_->prefix(spaceId, partId, prefix, &iter);
    if (ret != ResultCode::SUCCEEDED) {
        LOG(INFO) << "[spaceId:" << spaceId << ", partId:" << partId << "] access prefix failed"
                  << ", error code:" << static_cast<int32_t>(ret);
        cb(data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
        return;
    }
    data.reserve(kReserveNum);
    int32_t batchSize = 0;
    while (iter && iter->valid()) {
        if (batchSize >= FLAGS_snapshot_batch_size) {
            if (cb(data, totalCount, totalSize, raftex::SnapshotStatus::IN_PROGRESS)) {
                data.clear();
                batchSize = 0;
            } else {
                LOG(INFO) << "[spaceId:" << spaceId << ", partId:" << partId
                          << "] callback invoked failed";
                return;
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
    cb(data, totalCount, totalSize, raftex::SnapshotStatus::DONE);
}
}  // namespace kvstore
}  // namespace nebula


