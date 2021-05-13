/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_FLUSHTASK_H_
#define STORAGE_ADMIN_FLUSHTASK_H_

#include "common/thrift/ThriftTypes.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class FlushTask : public AdminTask {
public:
    FlushTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_FLUSHTASK_H_
