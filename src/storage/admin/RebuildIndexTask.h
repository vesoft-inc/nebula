/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_REBUILDINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDINDEXTASK_H_

#include "kvstore/LogEncoder.h"
#include "common/meta/IndexManager.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

using IndexItems = std::vector<std::shared_ptr<meta::cpp2::IndexItem>>;

class RebuildIndexTask : public AdminTask {
public:
    explicit RebuildIndexTask(StorageEnv* env, TaskContext&& ctx)
        : AdminTask(env, std::move(ctx)) {}

    ~RebuildIndexTask() {
        LOG(INFO) << "Release Rebuild Task";
    }

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

protected:
    virtual StatusOr<IndexItems>
    getIndexes(GraphSpaceID space) = 0;

    virtual StatusOr<std::shared_ptr<meta::cpp2::IndexItem>>
    getIndex(GraphSpaceID space, IndexID index) = 0;

    virtual nebula::cpp2::ErrorCode
    buildIndexGlobal(GraphSpaceID space,
                     PartitionID part,
                     const IndexItems& items) = 0;

    void cancel() override {
        canceled_ = true;
    }

    nebula::cpp2::ErrorCode
    buildIndexOnOperations(GraphSpaceID space, PartitionID part);


    // Remove the legacy operation log to make sure the index is correct.
    nebula::cpp2::ErrorCode
    removeLegacyLogs(GraphSpaceID space, PartitionID part);

    nebula::cpp2::ErrorCode
    writeData(GraphSpaceID space,
              PartitionID part,
              std::vector<kvstore::KV> data);

    nebula::cpp2::ErrorCode
    removeData(GraphSpaceID space,
               PartitionID part,
               std::string&& key);

    nebula::cpp2::ErrorCode
    cleanupOperationLogs(GraphSpaceID space,
                         PartitionID part,
                         std::vector<std::string> keys);

    nebula::cpp2::ErrorCode
    invoke(GraphSpaceID space,
           PartitionID part,
           const IndexItems& items);

protected:
    std::atomic<bool>   canceled_{false};
    GraphSpaceID        space_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_REBUILDINDEXTASK_H_
