/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_COMPACTTASK_H_
#define STORAGE_ADMIN_COMPACTTASK_H_

#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to do compact tasks.
 *
 */
class CompactTask : public AdminTask {
 public:
  CompactTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  bool check() override;

  /**
   * @brief Generate subtasks for compact.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

  nebula::cpp2::ErrorCode subTask(nebula::kvstore::KVEngine* engine);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_COMPACTTASK_H_
