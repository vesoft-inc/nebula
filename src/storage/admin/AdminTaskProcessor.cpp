/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/AdminTaskProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "interface/gen-cpp2/common_types.h"
#include "storage/admin/AdminTaskManager.h"

namespace nebula {
namespace storage {
void AdminTaskProcessor::process(const cpp2::AddAdminTaskRequest& req) {
  auto taskManager = AdminTaskManager::instance();

  auto cb = [taskManager, jobId = req.get_job_id(), taskId = req.get_task_id()](
                nebula::cpp2::ErrorCode errCode, nebula::meta::cpp2::StatsItem& result) {
    taskManager->saveAndNotify(jobId, taskId, errCode, result);
  };

  TaskContext ctx(req, std::move(cb));
  auto task = AdminTaskFactory::createAdminTask(env_, std::move(ctx));
  if (task) {
    nebula::meta::cpp2::StatsItem statsItem;
    statsItem.set_status(nebula::meta::cpp2::JobStatus::RUNNING);
    taskManager->saveTaskStatus(
        ctx.jobId_, ctx.taskId_, nebula::cpp2::ErrorCode::E_TASK_EXECUTION_FAILED, statsItem);
    taskManager->addAsyncTask(task);
  } else {
    cpp2::PartitionResult thriftRet;
    thriftRet.set_code(nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA);
    codes_.emplace_back(std::move(thriftRet));
  }
  onFinished();
}

void AdminTaskProcessor::onProcessFinished(nebula::meta::cpp2::StatsItem& result) {
  resp_.set_stats(std::move(result));
}

}  // namespace storage
}  // namespace nebula
