/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_REBUILDFTINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDFTINDEXTASK_H_

#include <utility>
#include <vector>

#include "common/base/ErrorOr.h"
#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace kvstore {
class Listener;
}  // namespace kvstore

namespace storage {
class StorageEnv;

class RebuildFTIndexTask : public AdminTask {
 public:
  RebuildFTIndexTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

 protected:
  nebula::cpp2::ErrorCode taskByPart(nebula::kvstore::Listener* listener);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDFTINDEXTASK_H_
