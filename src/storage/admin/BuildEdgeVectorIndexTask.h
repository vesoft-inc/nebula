/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_BUILDEDGEVECTORINDEXTASK_H_
#define STORAGE_ADMIN_BUILDEDGEVECTORINDEXTASK_H_

#include "common/thrift/ThriftTypes.h"
#include "storage/BaseProcessor.h"
#include "storage/admin/BuildVectorIndexTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to rebuild tag index.
 *
 */
class BuildEdgeVectorIndexTask : public BuildVectorIndexTask {
 public:
  BuildEdgeVectorIndexTask(StorageEnv* env, TaskContext&& ctx)
      : BuildVectorIndexTask(env, std::move(ctx)) {}

 private:
  /**
   * @brief Get the Index item by space id and index.
   *
   * @param space Space id.
   * @param index Index of the item.
   * @return StatusOr<std::shared_ptr<meta::cpp2::AnnIndexItem>>
   */
  StatusOr<std::shared_ptr<AnnIndexItem>> getIndex(GraphSpaceID space, IndexID index) override;

  /**
   * @brief Rebuilding index.
   *
   * @param space space id.
   * @param part Partition id.
   * @param items Index items.
   * @param rateLimiter Rate limiter of kvstore.
   * @return nebula::cpp2::ErrorCode Errorcode.
   */
  nebula::cpp2::ErrorCode buildIndexGlobal(GraphSpaceID space,
                                           PartitionID part,
                                           const std::shared_ptr<AnnIndexItem>& items,
                                           kvstore::RateLimiter* rateLimiter) override;

  nebula::cpp2::ErrorCode buildAnnIndex(GraphSpaceID space,
                                        PartitionID part,
                                        const std::shared_ptr<AnnIndexItem>& items,
                                        const VecData* data) override;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_BUILDEDGEVECTORINDEXTASK_H_
