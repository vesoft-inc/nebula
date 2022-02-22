/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_REBUILDTAGINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDTAGINDEXTASK_H_

#include "storage/BaseProcessor.h"
#include "storage/admin/RebuildIndexTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to rebuild tag index.
 *
 */
class RebuildTagIndexTask : public RebuildIndexTask {
 public:
  RebuildTagIndexTask(StorageEnv* env, TaskContext&& ctx) : RebuildIndexTask(env, std::move(ctx)) {}

 private:
  /**
   * @brief Get the Index Items by space id
   *
   * @param space Space id.
   * @return StatusOr<IndexItems> Vector of Index Items or status.
   */
  StatusOr<IndexItems> getIndexes(GraphSpaceID space) override;

  /**
   * @brief Get the Index item by space id and index.
   *
   * @param space Space id.
   * @param index Index of the item.
   * @return StatusOr<std::shared_ptr<meta::cpp2::IndexItem>>
   */
  StatusOr<std::shared_ptr<meta::cpp2::IndexItem>> getIndex(GraphSpaceID space,
                                                            IndexID index) override;

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
                                           const IndexItems& items,
                                           kvstore::RateLimiter* rateLimiter) override;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_REBUILDTAGINDEXTASK_H_
