/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_REBUILDINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDINDEXTASK_H_

#include <stddef.h>  // for size_t

#include <algorithm>  // for max
#include <atomic>     // for atomic
#include <memory>     // for shared_ptr
#include <vector>     // for vector

#include "common/base/ErrorOr.h"   // for ErrorOr
#include "common/base/Logging.h"   // for LOG, LogMessage, _LOG_INFO
#include "common/base/StatusOr.h"  // for StatusOr
#include "common/meta/IndexManager.h"
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID, PartitionID
#include "common/utils/Types.h"               // for IndexID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::E...
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/Common.h"  // for KV
#include "kvstore/LogEncoder.h"
#include "kvstore/RateLimiter.h"
#include "storage/admin/AdminTask.h"  // for AdminSubTask (ptr only)

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
class BatchHolder;
class RateLimiter;

class BatchHolder;
class RateLimiter;
}  // namespace kvstore
namespace meta {
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta

namespace storage {
class StorageEnv;

using IndexItems = std::vector<std::shared_ptr<meta::cpp2::IndexItem>>;

class RebuildIndexTask : public AdminTask {
 public:
  RebuildIndexTask(StorageEnv* env, TaskContext&& ctx);

  ~RebuildIndexTask() {
    LOG(INFO) << "Release Rebuild Task";
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

 protected:
  virtual StatusOr<IndexItems> getIndexes(GraphSpaceID space) = 0;

  virtual StatusOr<std::shared_ptr<meta::cpp2::IndexItem>> getIndex(GraphSpaceID space,
                                                                    IndexID index) = 0;

  virtual nebula::cpp2::ErrorCode buildIndexGlobal(GraphSpaceID space,
                                                   PartitionID part,
                                                   const IndexItems& items,
                                                   kvstore::RateLimiter* rateLimiter) = 0;

  void cancel() override {
    canceled_ = true;
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, nebula::cpp2::ErrorCode::E_USER_CANCEL);
  }

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
