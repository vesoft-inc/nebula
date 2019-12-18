/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "kvstore/SnapshotManagerImpl.h"
#include "base/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"

DEFINE_int32(snapshot_batch_size, 1024 * 1024 * 10, "batch size for snapshot");

namespace nebula {
namespace kvstore {

void SnapshotManagerImpl::accessAllRowsInSnapshot(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  raftex::SnapshotCallback cb) {
    CHECK_NOTNULL(store_);
    std::unique_ptr<KVIterator> iter;
    auto prefix = NebulaKeyUtils::prefix(partId);
    store_->prefix(spaceId, partId, prefix, &iter);
    std::vector<std::string> data;
    data.reserve(1024);
    int32_t batchSize = 0;
    int64_t totalSize = 0;
    int64_t totalCount = 0;
    while (iter && iter->valid()) {
        if (batchSize >= FLAGS_snapshot_batch_size) {
            cb(std::move(data), totalCount, totalSize, false);
            data.clear();
            batchSize = 0;
        }
        auto key = iter->key();
        auto val = iter->val();
        data.emplace_back(encodeKV(key, val));
        batchSize += data.back().size();
        totalSize += data.back().size();
        totalCount++;
        iter->next();
    }
    cb(std::move(data), totalCount, totalSize, true);
}
}  // namespace kvstore
}  // namespace nebula


