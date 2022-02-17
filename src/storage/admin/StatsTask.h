/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_STATSTASK_H_
#define STORAGE_ADMIN_STATSTASK_H_

#include <folly/concurrency/ConcurrentHashMap.h>  // for ConcurrentHashMap
#include <stddef.h>                               // for size_t

#include <atomic>         // for atomic
#include <string>         // for string, basic_string
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/base/Logging.h"              // for LOG, LogMessage
#include "common/thrift/ThriftTypes.h"        // for PartitionID, EdgeType
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCod...
#include "interface/gen-cpp2/meta_types.h"    // for StatsItem
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"  // for AdminTask, AdminSub...

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class StatsTask : public AdminTask {
 public:
  using AdminTask::finish;
  StatsTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  ~StatsTask() {
    LOG(INFO) << "Release Stats Task";
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

  void finish(nebula::cpp2::ErrorCode rc) override;

 protected:
  void cancel() override {
    canceled_ = true;
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, nebula::cpp2::ErrorCode::E_USER_CANCEL);
  }

  nebula::cpp2::ErrorCode genSubTask(GraphSpaceID space,
                                     PartitionID part,
                                     std::unordered_map<TagID, std::string> tags,
                                     std::unordered_map<EdgeType, std::string> edges);

 private:
  nebula::cpp2::ErrorCode getSchemas(GraphSpaceID spaceId);

 protected:
  GraphSpaceID spaceId_;

  // All tagIds and tagName of the spaceId
  std::unordered_map<TagID, std::string> tags_;

  // All edgeTypes and edgeName of the spaceId
  std::unordered_map<EdgeType, std::string> edges_;

  folly::ConcurrentHashMap<PartitionID, nebula::meta::cpp2::StatsItem> statistics_;

  // The number of subtasks equals to the number of parts in request
  size_t subTaskSize_{0};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_STATSTASK_H_
