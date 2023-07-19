/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_STATSTASK_H_
#define STORAGE_ADMIN_STATSTASK_H_

#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to handle statistics.
 *
 */
class StatsTask : public AdminTask {
 public:
  using AdminTask::finish;
  StatsTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  ~StatsTask() {
    LOG(INFO) << "Release Stats Task";
  }

  bool check() override;

  /**
   * @brief Generate sub tasks for StatsTask.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> Task vector or errorcode.
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

  /**
   * @brief Handle task execution result.
   *
   * @param rc Errorcode of task.
   */
  void finish(nebula::cpp2::ErrorCode rc) override;

 protected:
  nebula::cpp2::ErrorCode genSubTask(GraphSpaceID space,
                                     PartitionID part,
                                     std::unordered_map<TagID, std::string> tags,
                                     std::unordered_map<EdgeType, std::string> edges);

 private:
  nebula::cpp2::ErrorCode getSchemas(GraphSpaceID spaceId);

  void sleepIfScannedSomeRecord(size_t& countToSleep);

 protected:
  GraphSpaceID spaceId_;

  // All tagIds and tagName of the spaceId
  std::unordered_map<TagID, std::string> tags_;

  // All edgeTypes and edgeName of the spaceId
  std::unordered_map<EdgeType, std::string> edges_;

  folly::ConcurrentHashMap<PartitionID, nebula::meta::cpp2::StatsItem> statistics_;

  // The number of subtasks equals to the number of parts in request
  size_t subTaskSize_{0};

  static constexpr size_t kRecordsToSleep{1000};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_STATSTASK_H_
