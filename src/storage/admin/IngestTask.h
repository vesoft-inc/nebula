/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_INGESTTASK_H_
#define STORAGE_ADMIN_INGESTTASK_H_

#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to handle storage ingest task.
 *
 */
class IngestTask : public AdminTask {
 public:
  IngestTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  bool check() override;

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_INGESTTASK_H_
