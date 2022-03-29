/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_REBUILDFTINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDFTINDEXTASK_H_

#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to rebuild FT index.
 *
 */
class RebuildFTIndexTask : public AdminTask {
 public:
  RebuildFTIndexTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  bool check() override;

  /**
   * @brief Generate subtasks for rebuilding FT index.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

 protected:
  nebula::cpp2::ErrorCode taskByPart(nebula::kvstore::Listener* listener);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDFTINDEXTASK_H_
