// Copyright (c) 2024 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef STORAGE_ADMIN_REBUILDVECTORINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDVECTORINDEXTASK_H_

#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to rebuild vector index.
 */
class RebuildVectorIndexTask : public AdminTask {
 public:
  RebuildVectorIndexTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  bool check() override;

  /**
   * @brief Generate subtasks for rebuilding vector index.
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

 protected:
  nebula::cpp2::ErrorCode taskByPart(nebula::kvstore::Listener* listener);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_REBUILDVECTORINDEXTASK_H_ 