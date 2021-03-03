/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_SNAPSHOTMANAGERIMPL_H_
#define KVSTORE_SNAPSHOTMANAGERIMPL_H_

#include "common/base/Base.h"
#include "kvstore/raftex/SnapshotManager.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace kvstore {

class SnapshotManagerImpl : public raftex::SnapshotManager {
public:
    explicit SnapshotManagerImpl(KVStore* kv) : store_(kv) {}

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
                     int64_t& totalSize);

    KVStore* store_;
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_SNAPSHOTMANAGERIMPL_H_

