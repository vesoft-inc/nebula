/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_REBUILDEDGEINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDEDGEINDEXTASK_H_

#include "storage/admin/RebuildIndexTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to rebuild edge index.
 *
 */
class RebuildEdgeIndexTask : public RebuildIndexTask {
 public:
  RebuildEdgeIndexTask(StorageEnv* env, TaskContext&& ctx)
      : RebuildIndexTask(env, std::move(ctx)) {}

 private:
  StatusOr<IndexItems> getIndexes(GraphSpaceID space) override;

  StatusOr<std::shared_ptr<meta::cpp2::IndexItem>> getIndex(GraphSpaceID space,
                                                            IndexID index) override;

  nebula::cpp2::ErrorCode buildIndexGlobal(GraphSpaceID space,
                                           PartitionID part,
                                           const IndexItems& items,
                                           kvstore::RateLimiter* rateLimiter) override;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_REBUILDEDGEINDEXTASK_H_
