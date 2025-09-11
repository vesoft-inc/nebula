/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_BUILDVECTORINDEXTASK_H_
#define STORAGE_ADMIN_BUILDVECTORINDEXTASK_H_

#include "common/meta/IndexManager.h"
#include "common/vectorIndex/VectorIndexUtils.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/RateLimiter.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

using AnnIndexItem = meta::cpp2::AnnIndexItem;

/**
 * @brief Task class to rebuild the index.
 *
 */
class BuildVectorIndexTask : public AdminTask {
 public:
  using AdminTask::finish;

  BuildVectorIndexTask(StorageEnv* env, TaskContext&& ctx);

  ~BuildVectorIndexTask() {
    LOG(INFO) << "Release Build Vector Index Task";
  }

  bool check() override;

  void finish(nebula::cpp2::ErrorCode rc) override;

  /**
   * @brief Generate subtasks for rebuilding index.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

 protected:
  virtual StatusOr<std::shared_ptr<AnnIndexItem>> getIndex(GraphSpaceID space, IndexID index) = 0;

  virtual nebula::cpp2::ErrorCode buildIndexGlobal(GraphSpaceID space,
                                                   PartitionID part,
                                                   const std::shared_ptr<AnnIndexItem>& item,
                                                   kvstore::RateLimiter* rateLimiter) = 0;

  virtual nebula::cpp2::ErrorCode buildAnnIndex(GraphSpaceID space,
                                                PartitionID part,
                                                const std::shared_ptr<AnnIndexItem>& items,
                                                const VecData* data) = 0;

  nebula::cpp2::ErrorCode invoke(GraphSpaceID space,
                                 PartitionID part,
                                 const std::shared_ptr<AnnIndexItem>& item);

  nebula::cpp2::ErrorCode writeData(GraphSpaceID space,
                                    PartitionID part,
                                    std::vector<kvstore::KV> data,
                                    size_t batchSize,
                                    kvstore::RateLimiter* rateLimiter);

 protected:
  GraphSpaceID space_;
  bool changedSpaceGuard_{false};
  std::map<PartitionID, std::set<VectorID>> partVectorIds_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_BUILDVECTORINDEXTASK_H_
