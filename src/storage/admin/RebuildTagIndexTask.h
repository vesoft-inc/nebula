/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_REBUILDTAGINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDTAGINDEXTASK_H_

#include <memory>   // for shared_ptr
#include <utility>  // for move

#include "common/base/StatusOr.h"             // for StatusOr
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID, PartitionID
#include "common/utils/Types.h"               // for IndexID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "storage/BaseProcessor.h"
#include "storage/admin/AdminTask.h"         // for TaskContext
#include "storage/admin/RebuildIndexTask.h"  // for IndexItems, RebuildInde...

namespace nebula {
namespace meta {
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta
namespace storage {
class StorageEnv;
}  // namespace storage

namespace kvstore {
class RateLimiter;

class RateLimiter;
}  // namespace kvstore
namespace meta {
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta

namespace storage {
class StorageEnv;

class RebuildTagIndexTask : public RebuildIndexTask {
 public:
  RebuildTagIndexTask(StorageEnv* env, TaskContext&& ctx) : RebuildIndexTask(env, std::move(ctx)) {}

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

#endif  // STORAGE_ADMIN_REBUILDTAGINDEXTASK_H_
