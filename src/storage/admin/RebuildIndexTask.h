/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_REBUILDINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDINDEXTASK_H_

#include "common/meta/IndexManager.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/RateLimiter.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

using IndexItems = std::vector<std::shared_ptr<meta::cpp2::IndexItem>>;

/**
 * @brief Task class to rebuild the index.
 *
 */
class RebuildIndexTask : public AdminTask {
 public:
  RebuildIndexTask(StorageEnv* env, TaskContext&& ctx);

  ~RebuildIndexTask() {
    LOG(INFO) << "Release Rebuild Task";
  }

  /**
   * @brief Generate subtasks for rebuilding index.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

 protected:
  virtual StatusOr<IndexItems> getIndexes(GraphSpaceID space) = 0;

  virtual StatusOr<std::shared_ptr<meta::cpp2::IndexItem>> getIndex(GraphSpaceID space,
                                                                    IndexID index) = 0;

  virtual nebula::cpp2::ErrorCode buildIndexGlobal(GraphSpaceID space,
                                                   PartitionID part,
                                                   const IndexItems& items,
                                                   kvstore::RateLimiter* rateLimiter) = 0;

  nebula::cpp2::ErrorCode buildIndexOnOperations(GraphSpaceID space,
                                                 PartitionID part,
                                                 kvstore::RateLimiter* rateLimiter);

  // Remove the legacy operation log to make sure the index is correct.
  nebula::cpp2::ErrorCode removeLegacyLogs(GraphSpaceID space, PartitionID part);

  nebula::cpp2::ErrorCode writeData(GraphSpaceID space,
                                    PartitionID part,
                                    std::vector<kvstore::KV> data,
                                    size_t batchSize,
                                    kvstore::RateLimiter* rateLimiter);

  nebula::cpp2::ErrorCode writeOperation(GraphSpaceID space,
                                         PartitionID part,
                                         kvstore::BatchHolder* batchHolder,
                                         kvstore::RateLimiter* rateLimiter);

  nebula::cpp2::ErrorCode invoke(GraphSpaceID space, PartitionID part, const IndexItems& items);

 protected:
  GraphSpaceID space_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_REBUILDINDEXTASK_H_
