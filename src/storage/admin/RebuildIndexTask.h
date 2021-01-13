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

    ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

protected:
    virtual StatusOr<IndexItems>
    getIndexes(GraphSpaceID space) = 0;

    virtual StatusOr<std::shared_ptr<meta::cpp2::IndexItem>>
    getIndex(GraphSpaceID space, IndexID index) = 0;

    virtual kvstore::ResultCode
    buildIndexGlobal(GraphSpaceID space,
                     PartitionID part,
                     const IndexItems& items) = 0;

    void cancel() override {
        canceled_ = true;
    }

    kvstore::ResultCode buildIndexOnOperations(GraphSpaceID space,
                                               PartitionID part);


    // Remove the legacy operation log to make sure the index is correct.
    kvstore::ResultCode removeLegacyLogs(GraphSpaceID space,
                                         PartitionID part);

    kvstore::ResultCode writeData(GraphSpaceID space,
                                  PartitionID part,
                                  std::vector<kvstore::KV> data);

    kvstore::ResultCode removeData(GraphSpaceID space,
                                   PartitionID part,
                                   std::string&& key);

    kvstore::ResultCode cleanupOperationLogs(GraphSpaceID space,
                                             PartitionID part,
                                             std::vector<std::string> keys);

    kvstore::ResultCode invoke(GraphSpaceID space,
                               PartitionID part,
                               const IndexItems& items);

protected:
    std::atomic<bool>   canceled_{false};
    GraphSpaceID        space_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDINDEXTASK_H_
