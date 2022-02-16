/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_FLUSHTASK_H_
#define STORAGE_ADMIN_FLUSHTASK_H_

#include <utility>
#include <vector>

#include "common/base/ErrorOr.h"
#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {
class StorageEnv;

class FlushTask : public AdminTask {
 public:
  FlushTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_FLUSHTASK_H_
