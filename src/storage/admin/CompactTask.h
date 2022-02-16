/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_COMPACTTASK_H_
#define STORAGE_ADMIN_COMPACTTASK_H_

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
class KVEngine;
}  // namespace kvstore

namespace storage {
class StorageEnv;

class CompactTask : public AdminTask {
 public:
  CompactTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

  nebula::cpp2::ErrorCode subTask(nebula::kvstore::KVEngine* engine);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_COMPACTTASK_H_
